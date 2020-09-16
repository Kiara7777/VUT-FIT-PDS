'''
Created on 8. 4. 2019

@author: Kiara
'''

import argparse
import sys

import socket
import socketserver
import threading
import time

import json
import bencode

import signal

from xmlrpc.server import SimpleXMLRPCServer

CONST_MAX_USHORT = 65535

class ParamPeer:
    def __init__(self, id, username, mIpv4, mPort, rIpv4, rPort):
        self.id = id
        self.username = username
        self.mIpv4 = mIpv4
        self.mPort = mPort
        self.rIpv4 = rIpv4
        self.rPort = rPort
        self.textId = 0
        self.ack = {}
        
class DBPeer:
    def __init__(self, username, ipv4, port):
        self.username = username
        self.ipv4 = ipv4
        self.port = port

class ThreadedUDPServer(socketserver.ThreadingMixIn, socketserver.UDPServer):
    pass       
        

class MyUDPHandler(socketserver.DatagramRequestHandler):
    
    def setup(self):
        pass
    

    def handle(self):

        data = self.request[0]

        try:
            x = bencode.decode(data)
        except bencode.BencodeDecodeError as e:
            sys.stderr.write("PEER: error: bencode decode\n")
            
        try:
            msgType = x["type"]
        except KeyError as e:
            sys.stderr.write("NODE: error: badMsgSyntax: not type\n")
            
        if msgType == "ack":
            ackMsg(x, self)
        elif msgType == "list":
            listMsg(x, self)
        elif msgType == "message":
            messageMsg(x, self)
        


    def finish(self):
        pass 
    
    
    
class MyUDPServer:
    def __init__(self, param):
        self.param = param
        self.server = ThreadedUDPServer((param.mIpv4, param.mPort), MyUDPHandler)
        self.getOK = False
        self.printlist = False
        self.listOK = False
        self.innerDB = []
        
    def sendMsg(self, msg, address):
        #print("POSILAM" + msg.decode())
        self.server.socket.sendto(msg, address)
        
        
    def msgGET(self): 
        getMsg = getList(self.param.textId)
        
        self.param.ack[str(self.param.textId)] = False
        help = self.param.textId
        self.param.textId = addTextID(self.param.textId) #aktualizuj citac
        
        
        self.sendMsg(getMsg, (self.param.rIpv4, self.param.rPort)) #posli zpravu na sveho NOODA
        
        time.sleep(2) #2s ceka na ACK
        
        if self.param.ack[str(help)] == False:
            sys.stderr.write("PEER: error: GETLIST: ACK: nedoslo\n")
        else:
            self.getOK = True
            del self.param.ack[str(help)]
        
        
    def msgLIST(self):
        self.msgGET()
        if self.getOK: #GET probehl bez problemu a byl povrzen
            self.printlist = True
            self.getOK = False
            
    def msgRECONNECT(self, ipv4, port):
        self.sendMsg(helloEnd(self.param), (self.param.rIpv4, self.param.rPort)) #ukoncujici zprava
        #novy registracni port
        self.param.rIpv4 = ipv4
        self.param.rPort = port
        #force new hello
        self.sendMsg(hello(self.param), (self.param.rIpv4, self.param.rPort))
        
    
    def msgMESSAGE(self, fromP, to, message):
        
        if self.param.username != fromP:
            sys.stderr.write("PEER: error: MESSAGE: username a parametr from nejsou stejne, zprava se neposle\n")
            return
        
        self.msgGET()
        time.sleep(3) #dej sanci tomu ukoncit a prijmou zpravy
        if self.getOK and self.listOK: #GET a LIST probehl bez problemu a byl povrzen
            kdo = isInList(self.innerDB, to)
            self.getOK = False
            self.listOK = False
            if kdo[0]: #je tam, posli mu ZPRAVU
                msg = createPeerMeg(fromP, to, message, self.param.textId)
                
                self.param.ack[str(self.param.textId)] = False
                help = self.param.textId
                self.param.textId = addTextID(self.param.textId) #aktualizuj citac
                 
                self.sendMsg(msg, (kdo[1], kdo[2]))
                print("POSLAL: FROM: " + fromP +"(me)" + " TO: " + to + " MSG: " + message)
                
                time.sleep(2) #2s ceka na ACK
                
                if self.param.ack[str(help)] == False:
                    sys.stderr.write("PEER: error: MESSAGE: ACK: nedoslo\n")
                else:
                    del self.param.ack[str(help)]
            else: #uzivatel nenalezen- CHYBA
                sys.stderr.write("PEER: error: MESSAGE: uzivatel nenalezen\n")
                return
                
                
def createPeerMeg(fromP, to, msg, extId):
    x = {"type":"message", "txid":extId, "from":fromP, "to":to, "message": msg}
    return bencode.encode(x)        
        
def isInList(innerDB, to):
    '''
    Zkontroluje vnitrni DB zda obsahuje informaci uzivateli, kteremu chci poslat zpravu
    Vrati True, pokud ho najde
    ''' 
    for peer in innerDB:
        if peer.username == to:
            return (True, peer.ipv4, peer.port)
        
    return (False, '0', 0) 
           
  
def ackMsg(msg, handler):
    '''
    Zpracuje ACK zpravu
    '''
    
    txid = msg['txid']
    if str(txid) in handler.server.my.param.ack:
        handler.server.my.param.ack[str(txid)] = True
    else:
        print("PEER: error: ACK: spatne txid\n")

def sendACK (msg, handler):
    id = msg['txid']
    
    ack = {"type":"ack", "txid":id}
    data = bencode.encode(ack) 
    handler.server.my.sendMsg(data, handler.client_address) #posli ZPET

def listMsg(x, handler):
    '''
    Zpracuje LIST ZPRAVU
    '''
    sendACK(x, handler)
    
    
    #pretvori ZPRAVU na vnitrni DB
    handler.server.my.innerDB.clear()
    peers = x['peers']
    for peer in peers.values(): 
        user = DBPeer(peer['username'], peer['ipv4'], peer['port'])
        handler.server.my.innerDB.append(user)
    
    time.sleep(2) #synchrnonizacni sleep TODO, PREDELEJ TO
        
    if handler.server.my.printlist == True: #vypsat peery na stdout
        print("PEER DB")
        for peer in handler.server.my.innerDB:
            print("USERNAME: " + peer.username + " IPV4: " + peer.ipv4 + " PORT: " + str(peer.port))
    
    handler.server.my.printlist = False
    handler.server.my.listOK = True

def messageMsg(x, handler):
    '''
    Zpracuje MESSAGE ZPRAVU
    '''
    sendACK(x, handler)
    fromP = x['from']
    to = x['to']
    message = x['message']
    print("PRIJATO: FROM: " + fromP + " TO: " + to + "(me)" + " MESSAGE: " + message)

'''
Zpracovani parametru
'''
def parseArg():
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', required=True, type = int, help = 'Unikatni identifikator')
    parser.add_argument('--username', required=True, help = 'Unikatni uzivatelske jmeno')
    parser.add_argument('--chat-ipv4', required=True, help = 'IPv4 adresa, na ktere peer nasloucha prijima zpravy od jinych peeru nebo nodu')
    parser.add_argument('--chat-port', required=True, type = int, help = 'Port, na ktere peer nasloucha prijima zpravy od jinych peeru nebo nodu')
    parser.add_argument('--reg-ipv4', required=True, help = 'IPv4 adresa reg uzlu, peer na ni bude zasila HELLO a GETLIST zpravy')
    parser.add_argument('--reg-port', required=True, type = int, help='Port reg uzlu, peer na ni bude zasila HELLO a GETLIST zpravy')
    args = parser.parse_args()
    
    return ParamPeer(args.id, args.username, args.chat_ipv4, args.chat_port, args.reg_ipv4, args.reg_port)


'''
Co 10s bude podila HELLO ZPRAVY
Zaroven take posila prvni HELLO ZPRAVU
'''
def sendHellos(udpServer):
    
    while True:
        helloMsg = hello(udpServer.param) #vytvor hello zpravu
        udpServer.param.textId = addTextID(udpServer.param.textId) #aktualizuj citac
        udpServer.sendMsg(helloMsg, (udpServer.param.rIpv4, udpServer.param.rPort))
        time.sleep(10)


'''
Funkce to spravuje txtID, pricitava pripadne resetujr
'''
def addTextID(txtID):
    if txtID > CONST_MAX_USHORT:
        txtID = 0
    else:
        txtID = txtID + 1
        
    return txtID


'''
Vytvori HELLO ZPRAVU a vrati jeji bencide kod
'''      
def hello(param):
    x = {"type":"hello", "txid":param.textId, "username":param.username, "ipv4":param.mIpv4, "port":param.mPort}
    return bencode.encode(x)

'''
Vytvori ukoncujici HELLO ZPRAVU a vrati jeji bencode kod
'''
def helloEnd(param):
    x = {"type":"hello", "txid":param.textId, "username":param.username, "ipv4":"0.0.0.0", "port":0}
    return bencode.encode(x)
  
'''
Vytvori GETLIST ZPRAVU a vraci jeji bencode
'''
def getList(textId):
    x = {"type":"getlist", "txid":textId}
    return bencode.encode(x)  

        
def main():
    
    #zpracovat parametry
    param = parseArg()
    
    #objekt serveru
    udpServer = MyUDPServer(param)
    udpServer.server.my = udpServer
    serverThread = threading.Thread(target=udpServer.server.serve_forever)
    serverThread.daemon = True
    serverThread.start()

    
    #nastaveni XML-RPC serveru
    serverRPC = SimpleXMLRPCServer(("localhost", 8000 + udpServer.param.id), allow_none=True, logRequests=False)
    serverRPC.register_instance(udpServer)

    serverRPCThread = threading.Thread(target=serverRPC.serve_forever)
    serverRPCThread.daemon = True
    serverRPCThread.start()
    

    #odesle HELLO a nastavi dalsi odesilani HELLO zprav co 10s
    timerThread = threading.Thread(target=sendHellos, args=(udpServer,))
    timerThread.daemon = True
    timerThread.start()
    

    '''
    Vnitrni funkce pro odchyceni ukoncovaciho signalu 
    Uvnitr main aby mel pristup k potrebnym promennym a nemuselo se tak pouzit global
    '''
    def signalEnd (sig, frame):
        print("ODHLASOVANI...")
        udpServer.sendMsg(helloEnd(udpServer.param), (udpServer.param.rIpv4, udpServer.param.rPort))
        udpServer.server.shutdown()
        print("PEER KONEC")
        sys.exit(0)
        
    signal.signal(signal.SIGINT, signalEnd)
    
    while True:
        exit_signal = input('TADY BY BYLO OVLADANI POMOCI STDIN KDYBY BYLO\n')     
         
if __name__ == '__main__':
    main()