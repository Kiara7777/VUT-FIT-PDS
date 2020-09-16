'''
Created on 8. 4. 2019

@author: Kiara
'''

import argparse
import sys

import socketserver
import threading
import time
import socket

import json
import bencode #Knihovna bencode, prevzata z https://pypi.org/project/bencode.py/

import signal

from xmlrpc.server import SimpleXMLRPCServer

CONST_MAX_USHORT = 65535

class Param:
    def __init__(self, id, ipv4, port):
        self.id = id
        self.ipv4 = ipv4
        self.port = port
        self.textId = 0
        self.ack = {}
        
class DBPeer:
    def __init__(self, username, ipv4, port, regIpv4):
        self.username = username
        self.ipv4 = ipv4
        self.port = port
        self.regAddr = regIpv4
        self.lastHello = 0
        self.lastAutorative = 0

class ThreadedUDPServer(socketserver.ThreadingMixIn, socketserver.UDPServer):
    pass       
        

class MyUDPHandler(socketserver.DatagramRequestHandler):
    
    def setup(self):
        pass 
       
    def handle(self):

        data = self.request[0]
        timeMsg = time.time()
        
        try:
            x = bencode.decode(data)
        except bencode.BencodeDecodeError as e:
            sys.stderr.write("NODE: error: bencode decode\n")
        
        try:
            msgType = x["type"]
        except KeyError as e:
            sys.stderr.write("NODE: error: badMsgSyntax: not type\n")
        
        if msgType == "hello":
            helloMsg(x, self, timeMsg)
        elif msgType == "getlist":
            id = getACK(x, self) #vytvor ACK
            what = createList(self, id) #vytvor LIST
            if what == 0: #poslalo se LIST, cekej na ACK
                time.sleep(2)
                if self.server.my.param.ack[str(id)] == False:
                    sys.stderr.write("NODE: error: LIST: ACK: nedoslo\n")
                del self.server.my.param.ack[str(id)]
        elif msgType == "ack":
            ackMsg(x, self)
        elif msgType == "update":
            if self.server.my.isRunning == False:
                self.server.my.updateThread.start()
                self.server.my.isRunning = True
            if self.server.my.isTimeTest == False:
                self.server.my.testAutoritativeThread.start()
                self.server.my.isTimeTest = True
            if self.client_address in self.server.my.neighbors: #zu jsem  z nim synchronizovan, pocka si na automaticky update, nic nebudu posilat zpet, jenom zpracovat
                updateUserNode(x, self, timeMsg)
            else: #nejsem s nim sparovana, ulozim si ho, zpracuju a poslu zpet
                self.server.my.neighbors.append(self.client_address)
                
                msg = createUpdate(self.server.my) #update zprava
                self.server.my.param.textId = addTextID(self.server.my.param.textId)
                self.server.my.sendMsg(msg, self.client_address) #odesli mu to zpet
                
                updateUserNode(x, self, timeMsg)
                
        elif msgType == "disconnect":
            id = getACK(x, self) #odesle ack
            #smaz zaznamy
            self.server.my.nodeList.remove(self.client_address)
            self.server.my.neighbors.remove(self.client_address)
            deleteAutoritative(self.server.my.userList, self.client_address)
            
            
        
    def finish(self):
        pass 
    
    
    
class MyUDPServer:
    def __init__(self, param):
        self.param = param
        self.server = ThreadedUDPServer((param.ipv4, param.port), MyUDPHandler)
        self.userList = []
        self.nodeList = [] #tohle je pro seznam komu mam posilat update
        self.neighbors = [] #tohle je pro seznam s kym jsem uz alespon jednou sparovany
        self.myAddress = self.server.server_address
        
        self.isRunning = False
        self.updateThread = threading.Thread(target=sendUpdates, args=(self,))
        self.updateThread.daemon = True
        
        self.isTimeTest = False
        self.testAutoritativeThread = threading.Thread(target=atoritativeTimer, args=(self,))
        self.testAutoritativeThread.daemon = True
        
    def sendMsg(self, msg, address):
        #print("Posilam: " + msg.decode())
        self.server.socket.sendto(msg, address)
        
    def msgDatabase(self):
        print("NODE DB")
        for peer in self.userList:
            print("USERNAME: " + peer.username + " IPV4: " + peer.ipv4 + " PORT: " + str(peer.port))
            
    def msgCONNECT(self, ipv4, port):
        msg = createUpdate(self) #update zprava
        self.param.textId = addTextID(self.param.textId) #aktualizuj citac

        
        if self.isRunning == False:
            self.updateThread.start()
            self.isRunning = True
        
        if self.isTimeTest == False:
            self.testAutoritativeThread.start()
            self.isTimeTest = True
            
        self.sendMsg(msg, (ipv4, port))
        
    def msgNEIGHBORDS(self):
        print("NEIGHBORDS DB")
        for n in self.neighbors:
            print("IP: " + n[0] + " PORT: " + str(n[1]))
            
    def msgDISCONNECT(self):
        helpack = []
        for n in self.neighbors: 
            msg = createDisconnect(self)
            self.param.ack[str(self.param.textId)] = False
            helpack.append(str(self.param.textId))
            self.param.textId = addTextID(self.param.textId) #aktualizuj citac
            self.sendMsg(msg, n)
        self.neighbors.clear()
        self.nodeList.clear()
        
        time.sleep(2) #cekani na ACK
        
        for i in helpack:
            if self.param.ack[i] == False:
                sys.stderr.write("NODE: error: DISCONNECT: ACK: nedoslo\n")
            del self.server.my.param.ack[i]
            
    def msgSYNC(self):
        for n in self.neighbors:
            msg = createUpdate(self) #update zprava
            self.param.textId = addTextID(self.param.textId) #aktualizuj citac
            self.sendMsg(msg, n)


def createDisconnect(udpServer):
    '''
    Vytvori disconnect zpravu
    '''
    x = {"type":"disconnect", "txid":udpServer.param.textId}
    return bencode.encode(x)
    

def atoritativeTimer(udpServer):
    '''
    Kontroluje zda nejakemu zaznamu nevyprchal cas
    '''
    
    while True: 
        time.sleep(6)
        timeNow = time.time()
    
        index = 0
        while index < len(udpServer.userList): 
            peer = udpServer.userList[index]
            
            if peer.regAddr != udpServer.myAddress: #neni to muj peer, je to tedy autoritativni zaznam
                timeLeft = timeNow - peer.lastAutorative
                if timeLeft >= 12: #zaznam nebyl potvrzen uz pres 12 s - ODSTRANIT
                    peerDel = udpServer.userList.pop(index)
                    index = 0 #po odstaneni zacni znovu TODO - popremyslej jestli jenom neodstranit a nevratit index o 1/2 zpet
                else:
                    index += 1
            else:
                index += 1
    
         
def sendUpdates(mUdpServer):
    '''
    Co 3 nebo 4 sekundy posle vsem zaregistrovanym noodum UPDATE
    '''
    while True:
        time.sleep(4)
        udpateMessage = createUpdate(mUdpServer) #vytvor update zpravu
        mUdpServer.param.textId = addTextID(mUdpServer.param.textId) #aktualizuj citac
        
        
        for node in mUdpServer.nodeList: 
            mUdpServer.sendMsg(udpateMessage, node)

def updateUserNode(msg, handler, timeMsg):
    '''
    Zpracuje UPDATE zpravu, bere v potaz jenom autoritativni zaznamy
    autoritativni zaznamy = client od ktereho mi zprava prisla, vemu si jenom jeho kllic
    z ostatnich zanamu si jenom vezmu hodnoty nodu a ulozim si do DB
    '''
    if handler.client_address in handler.server.my.neighbors: #je v seznamu zesynchrnonizovanych?
        pass
    else:
        handler.server.my.neighbors.append(handler.client_address)
        
        
    if handler.client_address in handler.server.my.nodeList: #je zaregistrovan k posilani update?
        pass
    else:
        handler.server.my.nodeList.append(handler.client_address)
        
    
    db = msg['db']
    if not db: #v DB nic neni, synchrnonizovano s prazdnou DB, nic nemenit, konec
        pass

    #neco v DB je
    #ziskej autoritativni zaznam adresa klienta, klic ve tvaru "IP,PORT"
    autoritative = handler.client_address[0] + "," + str(handler.client_address[1])
    

    
    if autoritative in db.keys(): #jeho zanamy PRIDAT/AKTUALIZOVAT/ODSTRANIT
        #ziskat jeho zaznamy
        autoritativeUsers = db[autoritative]
        
        index = 0
        jeTam = False
        odstranit = {}
        
        while index < len(handler.server.my.userList):
            peer = handler.server.my.userList[index]
            if peer.regAddr == handler.client_address: #muj zazznam, projit  dict a hledat dany zaznam
                for key, value in autoritativeUsers.items(): #prohledat slovnik jestli tam dany zaznam je                         
                    if value['username'] == peer.username and value['ipv4'] == peer.ipv4 and value['port'] == peer.port: #ano zaznam je a nezmeneny - odstran ze slovniku
                        peer.lastAutorative = timeMsg
                        odstranit[key] = True
                        jeTam = True
                        index += 1
                        break
                    elif value['username'] == peer.username and (value['ipv4'] != peer.ipv4 or value['port'] != peer.port): #zaznam tam je, ale je zmeneny - AKTUALIZOVAT + ODSTRANIT
                        peer.ipv4 = value['ipv4']
                        peer.port = value['port']
                        peer.lastAutorative = timeMsg
                        odstranit[key] = True
                        jeTam = True
                        break
                
                #pro dany zaznam neni odpovidajici/zmeneny ve update slovniku - SMAZAT ZE SEZNAMU
                if jeTam == False:
                    del handler.server.my.userList[index]
                    index = 0
            else:#neni to muj zaznam, pokracuj dale
                index += 1
                
                
                    
        #prohledalo se cely zaznam, v odstranit jsou ve klicit ulozene klice, ktere se maji ignorovat - TO CO ZUSTANE ULOZIT DO userLISTU
        for key in odstranit.keys():
            del autoritativeUsers[key]
            
        for key, value in autoritativeUsers.items():
            peer = DBPeer(value['username'], value['ipv4'], value['port'], handler.client_address)
            peer.lastAutorative = timeMsg
            peer.lastHello = 0
            handler.server.my.userList.append(peer)
     
    else: #vubec nema autoritativni - smazat jeho zaznamy 
        deleteAutoritative(handler.server.my.userList,  handler.client_address)                    
    
    #yaregistruj k posilani UPDATE
    for key in db.keys():
        aList = key.split(',')
        addrN = (aList[0], int(aList[1]))
        if addrN in handler.server.my.nodeList: #uz tam je, neobtezuj se
            pass
        elif addrN == handler.server.my.myAddress:
            pass
        else:
            handler.server.my.nodeList.append(addrN)

def deleteAutoritative(userList, autoritative):
    '''
    Odstrani zaznamy daneho autoritativniho uzlu
    '''
    index = 0
    while index < len(userList):
        peer = userList[index]
        if peer.regAddr == autoritative:
            del userList[index]
            index = 0
        else:
            index += 1
        
            
def helloMsg(msg, self, timeHello):
    '''
    Zracuje HELLO ZPRAVY, TODO: mozna to bude chtit zamnky
    '''
    
    username = msg['username']
    ip = msg['ipv4']
    port = msg['port']
    
    if ip != '0.0.0.0' and port != 0: #registrace nebo zmena
        if len(self.server.my.userList) == 0: #v DB nic neni, proste to vlozime
            peer = DBPeer(username, ip, port, self.server.my.myAddress)
            peer.lastHello = timeHello
            self.server.my.userList.append(peer)
        else: #uz tam neco je
            jeTam = False
            index = 0
            while index < len(self.server.my.userList):
                
                peer = self.server.my.userList[index]
                if peer.username == username: #pro daneho uzivatele uz tam je zaznam - kouknu se jestli udaje jsou stejne, jinak AKTUALIZOVAT
                    
                    peer.ipv4 = ip
                    peer.port = port
                    peer.regAddr = self.server.my.myAddress
                    peer.lastHello = timeHello
                    
                    jeTam = True
                    break
                index += 1
                
            if jeTam == False: #v DB nenasel zaznam - PRIDAT HO TAM
                peer = DBPeer(username, ip, port, self.server.my.myAddress)
                peer.lastHello = timeHello
                self.server.my.userList.append(peer)
                
    else: #odstraneni zaznamu, ODREGISTRACE
        index = 0
        while index < len(self.server.my.userList):
            peer = self.server.my.userList[index]
            if peer.username == username:
                peerDel = self.server.my.userList.pop(index)
                break
            index += 1          

          
def dbTimer(udpServer):
    '''
    Co 15s zkontroluje zda nekomu nevypsel TIMEOUT TODO: ROZHODNI SE JAK CASTO TO KONTROLOVAT
    '''
    
    while True: 
        timeNow = time.time()
    
        index = 0
        peer = None
        while index < len(udpServer.userList):
            try: 
                peer = udpServer.userList[index]
            except:
                break
            
            if peer.regAddr != udpServer.myAddress: #neni to muj peer
                continue
            
            timeLeft = timeNow - peer.lastHello
            if timeLeft >= 30: #zaznam je starsi nez 30s, ODSTRANIT
                peerDel = udpServer.userList.pop(index)
                index = 0 #po odstaneni zacni znovu TODO - popremyslej jestli jenom neodstranit a nevratit index o 1/2 zpet
            else:
                index += 1 

        time.sleep(15)


def getACK(msg, handler):
    '''
    Zpracujje ZPRAVU a VYTVORI ACK NA NI a POSLE JI
    ''' 
    
    id = msg['txid']
    
    ack = {"type":"ack", "txid":id}
    data = bencode.encode(ack) 
    handler.server.my.sendMsg(data, handler.client_address) #posli ZPET
    
    return id
      
      
def parseArg():
    '''
    Zracuje argumenty prikazove radky a vrati je v objektu tridy Param
    '''
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', required=True, type = int, help = 'Unikatni identifikator')
    parser.add_argument('--reg-ipv4', required=True, help = 'IPv4 adresa reg uzlu, prijima registrace peeru a synchronizasi DB s dalsimi uzly')
    parser.add_argument('--reg-port', required=True, type = int, help='Port reg uzlu, prijima registrace peeru a synchronizasi DB s dalsimi uzly')
    args = parser.parse_args()
    
    return Param(args.id, args.reg_ipv4, args.reg_port)

def createList(handler, id):
    '''
    Zpracuje seznam registrovanych uzivatelu a vrati zpravu list
    '''
    isMine = False
    pocet = 0
    users = {}
    clientAddr = handler.client_address
    
    # zjisteni zda je dotaz od meho klienta + tvorba zpravy
    for peer in handler.server.my.userList:
        if peer.ipv4 == clientAddr[0] and peer.port == clientAddr[1] and peer.regAddr == handler.server.my.myAddress:
            isMine = True
        users[str(pocet)] = {"username":peer.username, "ipv4":peer.ipv4, "port":peer.port}
        pocet += 1
        
    if isMine: # pozadavek byl od registrovaneho, poslu mu to
        rest = {"type":"list", "txid":id, "peers":users}
        bRest = bencode.encode(rest)
        handler.server.my.sendMsg(bRest, clientAddr)
        handler.server.my.param.ack[str(id)] = False
        return 0
    else: #pozadavek od ciziho, SORRY NIC PRO TEBE - ERROR
        errorMsg = {"type":"error", "txid":id, "verbose":"NODE: LIST: nezaregistrovany uzivatel, pristup odepren"}
        bError = bencode.encode(errorMsg)
        handler.server.my.sendMsg(bError, clientAddr)
        return 1
    
    
def createUpdate(mUdpServer):
    '''
    Vytvori zpravu UPDATE
    '''
    DB = {}
    nodes = {}
    pocet = 0
    
    for peer in mUdpServer.userList:
        addr = peer.regAddr #adresa a port jsou klic, predela na string
        addrStr = addr[0] + "," + str(addr[1])
        
        if addrStr in nodes.keys(): #klic uz tam je, ziskej poradi
            pocet = nodes[addrStr]
        else: #pridat tam klic
            nodes[addrStr] = 0
            pocet = 0
            
        DB[addrStr] = {str(pocet): {"username":peer.username, "ipv4":peer.ipv4, "port":peer.port}}
        nodes[addrStr] += 1
        
    rest = {"type":"update", "txid":mUdpServer.param.textId, "db":DB}
    return bencode.encode(rest)
    
    
def ackMsg(msg, handler):
    '''
    Zpracuje ACK zpravu
    '''
    txid = msg['txid']
    if str(txid) in handler.server.my.param.ack:
        handler.server.my.param.ack[str(txid)] = True
    else:
        sys.stderr.write("NODE: error: ACK: nezmane\n")


def addTextID(txtID):
    '''
    Funkce to spravuje txtID, pricitava pripadne resetujr
    '''
    
    if txtID > CONST_MAX_USHORT:
        txtID = 0
    else:
        txtID = txtID + 1
        
    return txtID

def main():
    param = parseArg()
    
    #objekt serveru
    udpServer = MyUDPServer(param)
    udpServer.server.my = udpServer
    serverThread = threading.Thread(target=udpServer.server.serve_forever)
    serverThread.daemon = True
    serverThread.start()
    
    #nastaveni XML-RPC serveru
    serverRPC = SimpleXMLRPCServer(("localhost", 7000 + udpServer.param.id), allow_none=True, logRequests=False)
    serverRPC.register_instance(udpServer)

    serverRPCThread = threading.Thread(target=serverRPC.serve_forever)
    serverRPCThread.daemon = True
    serverRPCThread.start()
    

    #kontrola aktivity jednotlivych peeru
    timerThread = threading.Thread(target=dbTimer, args=(udpServer,))
    timerThread.daemon = True
    timerThread.start()

    
    '''
    Vnitrni funkce pro odchyceni ukoncovaciho signalu 
    Uvnitr main aby mel pristup k potrebnym promennym a nemuselo se tak pouzit global
    '''
    def signalEnd (sig, frame):
        print("ODHLASOVANI...")
        udpServer.msgDISCONNECT()
        udpServer.server.shutdown()
        print("NODE KONEC")
        sys.exit(0)
        
    signal.signal(signal.SIGINT, signalEnd)
    
    
    while True:
        exit_signal = input('TADY BY BYLO OVLADANI POMOCI STDIN KDYBY BYLO\n')
    
    

if __name__ == '__main__':
    main()