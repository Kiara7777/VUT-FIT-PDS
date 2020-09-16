# VUT FIT PDS

Projekt do předmětu Přenos dat, počítačové sítě a protokoly na VUT FIT 2. semestr, léto, magisterské studium

Hybridní chatovací P2P síť

## ZADÁNÍ:
1. Naprogramujte P2P chatovacího peera (9 bodů);
2. Naprogramujte P2P registrační uzel (9 bodů);
3. Zdokumentujte implementované aplikace a proveďte validační a verifikační testování kompatibility s 4 dalšími spolužáky (7 bodů); ** !NENI! **
4. BONUS: Implementované aplikace rozšiřte o použitelné GUI (4 body).

## DOPORUČENÍ/OMEZENÍ:

- Povolenými programovacími jazyky na tento projekt jsou C/C++, C#, Java a Python;
- Implementované aplikace budou primárně ovládány skrz specifikované CLI. Lze získat bonusové body za vytvoření použitelného GUI (ať už grafického či konzolového) k implementovaným peerům a uzlům;
- Uzel i peer mají nabindované porty pro různé typy zpráv na právě jedné IP adrese;
- Při provozu větší instance této hybridní sítě zajistěte provoz uzlů a peerů na veřejných IPv4 adresách, jinak totiž budete narážet pravděpodobně na implikace související s NATem (který níže uvedený protokol neřeší). V rámci testování můžete NAT omezení obejít například port-forwardingem na routeru ve vaší správě;
- Uzel i peer jsou implementováni jako démoni, kteří běží neustále a kteří po obdržení RPC pokynu vykonají nějakou akci (obvykle odešlou požadovanou zprávu). Povinně se musí ukončit na Ctrl+C / SIGINT;
- Nemusíte se zabývat autentizací peerů či nodů a vůbec vrozenou nezabezpečeností komunikačního protokolu a implementovaných programů;
- V rámci dokumentace máte ověřit kompatibilitu projektu s dalšími spolužáky. Nabádáme vás, ať neváháte použít fórum k projektu k synchronizaci toho, na kterých portech a adresách komu běží jaká aplikace;
- Unikátnost některých parametrů obecně v implementaci neřešte, jen ji předpokládejte.

## UPŘESNĚNÍ:

### Ad 1)

**PŘIPOJENÍ/ODPOJENÍ PEERA**

Po spuštění se peer připojí k právě jednomu registračnímu uzlu pomocí zprávy HELLO. Ve zprávě HELLO posílá svůj username (který ostatní používají při zasílání chatových zpráv), adresu IP a port (na kterém peer naslouchá k příjmu chatových zpráv od ostatních). Následně peer zprávu HELLO se stejnými parametry zasílá registračnímu uzlu každých 10s pro udržení spojení. Při ukončení aplikace peer odesílá HELLO zprávu s nulovou IP a číslem portu, čímž uzlu indikuje, že se odregistrovává ze sítě.

**CHATOVÁNÍ**

Když chce peer odeslat chatovou zprávu jinému peerovi, tak požádá svůj registrační uzel o aktuální údaje jiného peera pomocí zprávy GETLIST. Odpovědí na tuto zprávu je zpráva LIST, která obsahuje mapování mezi username a IP+port jak peerů zaregistrovaných k tomuto uzlu, tak i peerů zaregistrovaných k jiným uzlům, se kterými je peerův registrační uzel propojen. Pokud odesílající peer neobdrží ve zprávě LIST údaje o příjemci (jeho IP a port), pak není schopen chatovou zprávu odeslat a odmítne takový pokyn. Pro přenos chatu je vytvořeno ad-hoc spojení mezi peery (od odesílajícího na IP a port příjemce), kde se chatová data přenáší ve zprávě MESSAGE.

**SPUŠTĚNÍ**

Funkcionalita peer démona bude dostupná v rámci spustitelného souboru pds18-peer, která při spuštění používá následující argumenty:
```                     
./pds18-peer --id <identifikátor> --username <user> --chat-ipv4 <IP> --chat-port <port> --reg-ipv4 <IP> --reg-port <port>
```
- --id je unikátní identifikátor instance peera pro případy, kdy je potřeba rozlišit mezi nimi v rámci jednoho hosta (operačního systému), na kterém běží
- --username je unikátní uživatelské jméno identifikující tohoto peera v rámci chatu;
- --chat-ipv4 a --chat-port je IP adresa a port, na kterém peer naslouchá a přijímá zprávy od ostatních peerů či nodů;
- --reg-ipv4 a --reg-port je IP adresa a port registračního uzlu, na který peer bude: 1) pravidelně zasílat HELLO zprávy; a 2) odesílat dotazy GETLIST k zjištění aktuálního mapování.
Chatovací zprávy se vypisují v rámci činnosti na stdout, diagnostické informace pak na stderr.

### Ad 2)

**PŘIPOJENÍ/ODPOJENÍ PEERA**

Registrační uzel si udržuje aktuální databázi peerů, které se k němu zaregistrovali. S příchodem každé HELLO zprávy aktualizuje údaje v této databázi pro daného peera. V případě přijetí HELLO zprávy s nulovými údaji (IP, port = 0, viz níže) odebírá peera (resp. uživatele specifikovaného v parametru username) z databáze, stejně tak ve chvíli, kdy od peera neuslyší žádnou HELLO zprávu po dobu delší než 30 vteřin.

**CHATOVÁNÍ**

Registrační uzly si udržují přehled o peerech existujících v síti v rámci databáze. Na dotazy GETLIST jen od svých peerů odpovídá uzel zprávami LIST, ve kterých jsou všechna dostupná mapování username peerů a jejich IP+port údajů (tzn. všech peerů v síti). Interně je registrační uzel schopen v této databázi rozlišit mezi svými peery (které se k němu registrují) a cizími (které se registrují k jiným uzlům). Za účelem tohoto rozlišení má každý záznam v databázi kromě údajů o mapování peera i IP adresu uzlu, který tohoto peera registruje.

**PŘIPOJENÍ/ODPOJENÍ UZLU**

Registrační uzel je schopen vytvořit si sousedství s jiným uzlem a vyměnit si informace ve svých databázích peerů. V rámci synchronizace databází si uzly ad-hoc (tj. na základě změny mapování peera) či pravidelně (tj. nejpozději 4 vteřiny od poslední synchronizace) zasílají UPDATE zprávy. V UPDATE zprávě zasílá uzel stav své databáze peerů. Ze své podstaty UPDATE zpráva obsahuje dva typy záznamů - autoritativní (záznamy o těch peerech, kteří se k danému uzlu registrují) a neautoritativní (záznamy o peerech zaregistrovaných k jiným uzlům a které jsou pouze zprostředkované). Při přijetí a zpracování UPDATE zprávy uzel aktualizuje ve své databázi jen autoritativní záznamy od sousedního uzlu. Z neautoritativních záznamů je tento registrační uzel schopen zjistit IP adresu dalšího uzlu a vytvořit si s ním v případě potřeby nové sousedství. Takto mezi registračními uzly vzniká full-mesh síť. V případě odpojení uzlu od sítě odesílá tento VŠEM svým sousedům DISCONNECT zprávu, kterou dává pokyn k odstranění záznamů z databáze peerů, pro které je autoritativní. K odstranění selektivního záznamu v databázi dojde také v případě, že uzel neuslyší od autoritativního uzlu žádnou novou UPDATE zprávu po dobu delší než 12 vteřin.

**SPUŠTĚNÍ**

Funkcionalita démona registračního uzlu bude dostupná v rámci spustitelného souboru pds18-node, který při spuštění používá následující argumenty:
```
./pds18-node --id <identifikátor> --reg-ipv4 <IP> --reg-port <port>
```
- --id je unikátní identifikátor instance peera pro případy, kdy je potřeba rozlišit mezi nimi v rámci jednoho hosta (operačního systému), na kterém běží
- --reg-ipv4 a --reg-port je IP adresa a port registračního uzlu, na kterém přijímá registrace peerů a synchronizace databáze ze sousedními uzly (zkrátka zprávy od peerů a nodů);

### Ad 3)

V dobré dokumentaci se očekává následující: titulní strana, obsah, logické strukturování textu, výběr relevantních informací z nastudované literatury, popis zajímavějších pasáží implementace, sekce o testování (ve které kromě vašeho vlastního programu otestujete jeho kompatibilitu s alespoň 4 dalšími spolužáky (jejichž loginy/jména povinně uvedete do readme a dokumentace) a jejich implementacemi preferovaně na jiných jazycích), bibliografie, popisy k řešení bonusových zadání.

### Ad 1+2)

**PROTOKOL**

Peeři i uzly používají jednoduchý komunikační protokol sestávající se z níže uvedených zpráv. Všechny zprávy mezi dvěma peery, dvěma uzly, či peerem vs. uzlem jsou přenášeny skrz UDP. Všechny zprávy mají JSON syntaxi, kde poviným atributem je "type", který specifikuje typ zprávy. Před přenosem v UDP je obsah zprávy bencodován. Protože UDP negarantuje doručení, tak k potvrzení se používá zpráva ACK, která v sobě nese odkaz na jedinečný transakční identifikátor zprávy, kterou potvrzuje. Pokud dojde při zpracování libovolné zprávy k chybě, odpovídá protistrana zprávou ERROR, která kromě identifikátoru transakce obsahuje i slovní popis problému, ke kterému došlo. Zprávy HELLO, UPDATE a ERROR není potřeba pomocí ACK potvrzovat. Na potvrzení ACK čekat max. 2 vteřiny, poté nejprve zresetovat stav zpracování související s nepotvrzenou zprávou a posléze ohlásit chybu na stderr (která by ale neměla v ideálním případě vést k pádu programu, jen notifikovat uživatele o tom, co se děje). Obecně lze zprávy mimo očekávaný stav protokolu zahazovat. Za účelem rozlišení ke které GETLIST zprávě LIST patří může odesilatel navazující LIST zopakovat TXID z předchozí GETLIST zprávy od příjemce.


Protokol podporuje následující zprávy:

```                       
HELLO := {"type":"hello", "txid":<ushort>, "username":"<string>", "ipv4":"<dotted_decimal_IP>", "port": <ushort>}                       
                       
GETLIST := {"type":"getlist", "txid":<ushort>}                       
                       
LIST := {"type":"list", "txid":<ushort>, "peers": {<PEER_RECORD*>}}                       
PEER_RECORD := {"<ushort>":{"username":"<string>", "ipv4":"<dotted_decimal_IP>", "port": <ushort>}}                       
                       
MESSAGE := {"type":"message", "txid":<ushort>, "from":"<string>", "to":"<string>", "message":"<string>"}                       
                       
UPDATE := {"type":"update", "txid":<ushort>, "db": {<DB_RECORD*>}}                       
DB_RECORD := {"<dotted_decimal_IP>,<ushort_port>":{<PEER_RECORD*>}}                       
                       
DISCONNECT := {"type":"disconnect", "txid":<ushort>}                       
                       
ACK := {"type":"ack", "txid":<ushort>}                       
                       
ERROR := {"type":"error", "txid":<ushort>, "verbose": "<string>"}
```

Ukázka zpráv a jejich zakódování:

```                       
HELLO1 := {"type":"hello", "txid":123, "username":"xlogin00", "ipv4": "192.0.2.1", "port": 34567}                       
BENCODED(HELLO1) := "d4:ipv49:192.0.2.14:porti34567e4:txidi123e4:type5:hello8:username8:xlogin00e"                       
                       
HELLO2 := {"type":"hello", "txid":123, "username":"xlogin00", "ipv4": "0.0.0.0", "port": 0}                       
BENCODED(HELLO2) := "d4:ipv47:0.0.0.04:porti0e4:txidi123e4:type5:hello8:username8:xlogin00e"                       
                       
GETLIST := {"type":"getlist", "txid":123}                       
BENCODED(GETLIST) := "d4:txidi123e4:type7:getliste"                       
                       
ERROR := {"type": "error", "txid":123, "verbose": "I refuse to send list of peers, requestor is not registered to me!"}                       
BENCODED(ERROR) := "d4:txidi123e4:type5:error7:verbose66:I refuse to send list of peers, requestor is not registered to me!e"                       
                       
LIST := {"type":"list", "txid":123, "peers": {"0":{"username":"xlogin00", "ipv4": "192.0.2.1", "port": 34567}, "1":{"username":"xnigol99", "ipv4": "192.0.2.2", "port": 45678}}}                       
BENCODED(LIST) := "d5:peersd1:0d4:ipv49:192.0.2.14:porti34567e8:username8:xlogin00e1:1d4:ipv49:192.0.2.24:porti45678e8:username8:xnigol99ee4:txidi123e4:type4:liste"                       
                       
MESSAGE := {"type":"message", "txid":123, "from":"xlogin00", "to":"xnigol99", "message": "blablabla"}                       
BENCODED(MESSAGE) := "d4:from8:xlogin007:message9:blablabla2:to8:xnigol994:txidi123e4:type7:messagee"                       
                       
UPDATE := {"type":"update", "txid":123, "db": {"192.0.2.198,12345": {"0":{"username":"xlogin00", "ipv4": "192.0.2.1", "port": 34567},"1":{"username":"xnigol99", "ipv4": "192.0.2.2", "port": 45678}}, "192.0.2.199,12345":{"0":{"username":"xtestx00", "ipv4": "192.0.2.3", "port": 65432}}}}            
BENCODED(UPDATE) := "d2:dbd17:192.0.2.198,12345d1:0d4:ipv49:192.0.2.14:porti34567e8:username8:xlogin00e1:1d4:ipv49:192.0.2.24:porti45678e8:username8:xnigol99ee17:192.0.2.199,12345d1:0d4:ipv49:192.0.2.34:porti65432e8:username8:xtestx00eee4:txidi123e4:type6:updatee"             
                       
DISCONNECT := {"type":"disconnect", "txid":123}                       
BENCODED(DISCONNECT) := "d4:txidi123e4:type10:disconnecte"                       
                       
ACK := {"type": "ack", "txid":123}                       
BENCODED(ACK) := "d4:txidi123e4:type3:acke"
```
### RPC

Je zcela na autorovi projektu, jaký způsob implementace RPC zvolí (např. zadání na stdout, pipe, telnetování příkazů). Aby však bylo možné projekt uniformě testovat, je potřeba dodat i separátní program pds18-rpc. Všechny implementované programy mohou vytvářet dočasné soubory za účelem předávání informací RPC aplikaci. Nicméně všechny dočasné soubory musí být po ukončení programu mazaný tak, aby v souborovém systému nezustaval nepořádek. RPC příkazy by měly být atomické (tzn. dělají jednu věc, typicky odesílají jednu zprávu) a jsou zde pro účely opravování, ale i skriptování chování aplikace. Nicméně implementované aplikace by měly fungovat sami bez sebe i bez jakékoli intervence ze strany RPC.

Očekává se následující spuštění tohoto programu:

```                      
./pds18-rpc --id <identifikátor> <"--peer"|"--node"> --command <příkaz> --<parametr1> <hodnota_parametru1> ...
```

- --id obsahuje identifikátor instance peera či uzlu, kterému se má RPC příkaz zaslat
- --peer či --node určuje, jestli se jedná o příkaz pro instanci peera či registračního uzlu
- --command a seznam parametrů určují příkaz a parametry vztahující se k danému RPC volání

Je povinnost implementovat následující příkazy s těmito parametry:

- --peer --command message --from <username1> --to <username2> --message <zpráva>, který se pokusí odeslat chat zprávu
- --peer --command getlist, který vynutí aktualizaci seznamu v síti známých peerů, tj. odešle zprávu GETLIST a nechá si ji potvrdit
- --peer --command peers, který zobrazí aktuální seznam peerů v síti, tj. peer si s node vymění zprávy GETLIST a LIST, přičemž obsah zprávy LIST vypíše
- --peer --command reconnect --reg-ipv4 <IP> --reg-port <port>, který se odpojí od současného registračního uzlu (nulové HELLO) a připojí se k uzlu specifikovaném v parametrech
- --node --command database, který zobrazí aktuální databázi peerů a jejich mapování
- --node --command neighbors, který zobrazí seznam aktuálních sousedů registračního uzlu
- --node --command connect --reg-ipv4 <IP> --reg-port <port>, který se pokusí navázat sousedství s novým registračním uzlem
- --node --command disconnect, který zruší sousedství se všemi uzly (stáhne z jejich DB své autoritativní záznamy) a odpojí node od sítě
- --node --command sync, která vynutí synchronizaci DB s uzly, se kterými node aktuálně sousedí
