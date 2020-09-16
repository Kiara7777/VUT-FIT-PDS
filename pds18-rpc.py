'''
Created on 8. 4. 2019

@author: Kiara
'''
import xmlrpc.client

import argparse
import sys


def parseArg():
    
    
    myChoices = ['message', 'getlist', 'peers', 'reconnect', 'database', 'neighbors', 'connect', 'disconnect', 'sync']
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', required=True, type = int, help = 'Unikatni identifikator')
    
    group = parser.add_mutually_exclusive_group(required=True) 
    group.add_argument('--peer', action='store_true', help = 'Pro koho je dany prikaz, nemuze byt dohromady s --node')
    group.add_argument('--node', action='store_true', help = 'Pro koho je dany prikaz, nemuze byt dohromady s --peer')
    
    parser.add_argument('--command', choices = myChoices, help = 'Druh prikazu')
    
    parser.add_argument('--from', dest = 'fromP', help = 'PEER: message: Uzivatelske jmeno odesilatele')
    parser.add_argument('--to', help = 'PEER: message: Uzivatelske jmeno prijemce')
    parser.add_argument('--message', help = 'PEER: message: Zprava co se ma odeslat')
    
    parser.add_argument('--reg-ipv4', help = 'PEER/NODE: reconnect/connect: Odpojeni od aktualniho uzlu a pripojeni ke zadamenu IPv4 adresou/Pokus o navazani sousedsstvi')
    parser.add_argument('--reg-port', type = int, help = 'PEER/NODE: reconnect/connect: Odpojeni od aktualniho uzlu a pripojeni ke zadamenu portem/Pokus o navazani sousedstvi')
   
    args = parser.parse_args()
    
    mess = args.command
    
    #peer nemuze byt s database a dale
    if(args.peer and (mess == 'database' or mess == 'neighbors' or mess == 'connect' or mess == 'disconnect' or mess == 'sync')):
        parser.error("--peer a --connect " + mess + " nemohou byt dohormady")
    
    #node nemuye byt dohromady message a dale
    if(args.node and (mess == 'message' or mess == 'getlist' or mess == 'peers' or mess == 'reconnect')):
        parser.error("--node a --connect " + mess + " nemohou byt dohormady")
        
    #tyhle zpravy nesmi mic tni dalsiho nastaveneho
    if ((mess == 'getlist' or mess == 'peers' or mess == 'database' or mess == 'neighbors' or mess == 'disconnect' or mess == 'sync') and 
        (args.fromP is not None or args.to is not None or args.message is not None or args.reg_ipv4 is not None or args.reg_port is not None)):
        parser.error("Neplatna kombinace druhu prikazu a dalsich parametru")
    
    #prikaz message portrebuje from, to a message, test zda neco nechybi nebo neco neprebyva
    if (mess == 'message' and (args.fromP is None or args.to is None or args.message is None)):
        parser.error("Chybejicic parametry u message")
    
    if (mess == 'message' and (args.reg_ipv4 is not None or args.reg_port is not None)):
        parser.error("U message nemuze byt --reg-ipv4 nebo --reg-port")
        
    #reconnect a connect potrebuji regipv4 a regPort
    if ((mess == 'reconnect' or mess == 'connect') and (args.reg_ipv4 is None or args.reg_port is None)):
        parser.error("Chybejicic parametry u reconnect nebo connect")

    if ((mess == 'reconnect' or mess == 'connect') and (args.fromP is not None or args.to is not None or args.message is not None)):
        parser.error("U reconnect/connect nemuze byt: --from, --to, --message") 
    return args

def createAdress(id, typeP):
    if (typeP == "peer"):
        port = 8000 + id
        return "http://localhost:" + str(port) + "/"
    else:
        port = 7000 + id
        return "http://localhost:" + str(port) + "/"
        
    
     
def main():
    args = parseArg()
    addr = ""
    
    if args.peer:
        addr = createAdress(args.id, "peer")
    elif  args.node:
        addr = createAdress(args.id, "node")
    else:
        sys.stderr.write("RPC: error: chyba v parametrech\n")
        

    with xmlrpc.client.ServerProxy(addr) as proxy:
        
        if args.command == 'getlist':
            proxy.msgGET()
        elif args.command == 'peers':
            proxy.msgLIST()
        elif args.command == 'reconnect':
            proxy.msgRECONNECT(args.reg_ipv4, args.reg_port)
        elif args.command == 'database':
            proxy.msgDatabase()
        elif args.command == 'message':
            proxy.msgMESSAGE(args.fromP, args.to, args.message)
        elif args.command == 'connect':
            proxy.msgCONNECT(args.reg_ipv4, args.reg_port)
        elif args.command == 'neighbors':
            proxy.msgNEIGHBORDS()
        elif args.command == 'disconnect':
            proxy.msgDISCONNECT()
        elif args.command =='sync':
            proxy.msgSYNC()
        else:
            sys.stderr.write("RPC: error: neznamy prikaz\n")
        
            

if __name__ == '__main__':
    main()