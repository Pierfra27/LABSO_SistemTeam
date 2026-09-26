import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
 
/**
 * Un thread per ogni nodo sensore connesso: legge una richiesta, la esegue e scrive la risposta, finche' la connessione non si chiude. 
 * La connessione e' persistente, quindi l'istanza dura quanto il nodo e puo' ricordare il suo peerId.
 *
 * E' il punto in cui sta TUTTA la gestione dell'aggregatore. Le classi di stato sono passive: ognuna protegge i propri dati e ritorna, ma non chiama mai le altre. 
 * Chiamarle in sequenza, una alla volta, e' compito di questo thread, ed e' cio' che garantisce meccanicamente che un thread non tenga mai due monitor insieme 
 * e non ne tenga mai uno mentre scrive su un socket.
 *
 * Questa classe non ha alcun monitor proprio e non ne serve uno: ogni istanza e' usata da un solo thread. 
 * L'unica eccezione e' chiudi(), che il thread della console chiama sul quit, e che tocca soltanto il socket.
 */
public class NodeHandler implements Runnable {
 
    /**
     * Richiesta sintatticamente sbagliata: campi mancanti, numero non parsabile, esito della RELEASE diverso da OK/FAIL.
     *
     * Esiste perche' Protocol.parseInt converte gli errori di formato in IOException, che e' la stessa eccezione di un socket morto: 
     * senza un tipo dedicato le due situazioni sarebbero indistinguibili, e vanno trattate in modo opposto. Una riga malformata deve
     * produrre ERR BADREQUEST lasciando la connessione viva, un socket morto deve far uscire dal ciclo.
     */
    private static class RichiestaMalformata extends Exception {
    }
 
    private final Socket socket;
    private final AggregatorState stato;
    private final TokenManager token;
    private final DownloadLog log;
    private final Aggregator aggregatore;
 
    /**
     * Null finche' non arriva la REGISTER: e' anche il modo in cui si riconosce un nodo non ancora registrato (ERR NOTREGISTERED) 
     * e una seconda REGISTER sulla stessa connessione (ERR BADREQUEST).
     */
    private String peerId;
 
    public NodeHandler(Socket socket, AggregatorState stato, TokenManager token,
                       DownloadLog log, Aggregator aggregatore) {
        this.socket = socket;
        this.stato = stato;
        this.token = token;
        this.log = log;
        this.aggregatore = aggregatore;
    }
 
    @Override
    public void run() {
        System.out.println("Connessione da " + socket.getInetAddress().getHostAddress());
        try {
            // Gli stream si prendono una volta sola: getInputStream ritorna sempre lo stesso oggetto, ma chiederlo a ogni giro 
            // nasconderebbe che il flusso e' uno solo e continuo
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();
            ciclo(in, out);
        } catch (IOException e) {
            // Vale sia per la lettura sia per la scrittura: la connessione non c'e' piu', non ha senso tentare di rispondere. 
            // Ci rientra anche la riga oltre gli 8 KB: dopo una riga troppo lunga lo stream non e' piu' affidabile.
        } catch (InterruptedException e) {
            // Arriva dalla wait() di token.acquisisci. Il flag di interruzione va ripristinato perche' catturare l'eccezione lo azzera, 
            // e chi ci eseguira' sopra altro codice deve poter sapere che un'interruzione e' stata richiesta.
            Thread.currentThread().interrupt();
        } finally {
            pulizia();
        }
    }
 
    /**
     * Il ciclo di servizio. Le due eccezioni che escono da qui (IOException e InterruptedException) significano entrambe "questa connessione e' finita"; 
     * tutto cio' che invece e' colpa della richiesta e non del canale diventa una risposta ERR e il ciclo prosegue.
     */
    private void ciclo(InputStream in, OutputStream out) throws IOException, InterruptedException {
        while (true) {
            String riga = Protocol.readLine(in);
            if (riga == null) {
                return; // chiusura pulita dall'altro capo: la pulizia la fa il finally
            }
 
            String[] campi = Protocol.split(riga);
            if (campi.length == 0) {
                Protocol.writeLine(out, errore(Protocol.ERR_BADREQUEST));
                continue;
            }
 
            try {
                if (!esegui(campi, in, out)) {
                    return;
                }
            } catch (RichiestaMalformata e) {
                Protocol.writeLine(out, errore(Protocol.ERR_BADREQUEST));
            }
        }
    }
 
    /**
     * Esegue una singola richiesta.
     *
     * @return true per continuare a servire la connessione, false per uscire dal ciclo
     */
    private boolean esegui(String[] campi, InputStream in, OutputStream out)
            throws IOException, InterruptedException, RichiestaMalformata {
 
        String comando = campi[0];
 
        // Il controllo sta prima del dispatch e non dentro ogni singolo ramo: la regola e' dello stesso tipo per tutti i comandi.
        if (peerId == null && !comando.equals(Protocol.REQ_REGISTER)) {
            Protocol.writeLine(out, errore(Protocol.ERR_NOTREGISTERED));
            return true;
        }
 
        if (comando.equals(Protocol.REQ_REGISTER)) {
            return register(campi, in, out);
        }
        if (comando.equals(Protocol.REQ_LIST)) {
            return list(campi, out);
        }
        if (comando.equals(Protocol.REQ_PEERS)) {
            return peers(out);
        }
        if (comando.equals(Protocol.REQ_ADDED)) {
            return added(campi, out);
        }
        if (comando.equals(Protocol.REQ_REMOVED)) {
            return removed(campi, out);
        }
        if (comando.equals(Protocol.REQ_RESOLVE)) {
            return resolve(campi, out);
        }
        if (comando.equals(Protocol.REQ_RESOLVE_AT)) {
            return resolveAt(campi, out);
        }
        if (comando.equals(Protocol.REQ_RELEASE)) {
            return release(campi, out);
        }
        if (comando.equals(Protocol.REQ_DISCONNECT)) {
            Protocol.writeLine(out, Protocol.RESP_OK);
            return false; // la pulizia e la chiusura del socket le fa il finally
        }
 
        throw new RichiestaMalformata();
    }
 
    /**
     * REGISTER portaAscolto n, seguita da n righe con i nomi delle risorse.
     *
     * Le n righe si leggono in una lista LOCALE, fuori da qualunque monitor, e solo quando ci sono tutte si chiama l'unico metodo synchronized completaRegistrazione. 
     *
     * L'host non lo dichiara il client ma lo ricava l'aggregatore dal socket: un indirizzo autodichiarato sarebbe sbagliato ogni volta che il client si sbaglia. 
     */
    private boolean register(String[] campi, InputStream in, OutputStream out)
            throws IOException, RichiestaMalformata {
 
        if (peerId != null) {
            // Una seconda REGISTER cambierebbe identita' a meta' dialogo: le richieste gia' servite resterebbero attribuite al peerId vecchio.
            Protocol.writeLine(out, errore(Protocol.ERR_BADREQUEST));
            return true;
        }
        if (campi.length < 3) {
            throw new RichiestaMalformata();
        }
 
        int porta = numero(campi[1]);
        int quante = numero(campi[2]);
        if (porta < 1 || porta > 65535 || quante < 0) {
            throw new RichiestaMalformata();
        }
 
        List<String> nomi = new ArrayList<>();
        for (int i = 0; i < quante; i++) {
            String nome = Protocol.readLine(in);
            if (nome == null) {
                // EOF fra l'intestazione e le n righe: si esce senza rispondere e senza toccare la tabella. Si perde solo il contenuto di questa lista locale.
                return false;
            }
            nome = nome.trim();
            if (!nome.isEmpty()) {
                nomi.add(nome);
            }
        }
 
        String host = socket.getInetAddress().getHostAddress();
        peerId = stato.completaRegistrazione(host, porta, nomi);
 
        Protocol.writeLine(out, Protocol.RESP_OK + " " + peerId);
        System.out.println("Registrato " + peerId + " (" + host + ":" + porta + ") con "
                + nomi.size() + " risorse");
        return true;
    }
 
    // LIST nome|- : il "-" diventa il null che AggregatorState intende come "tutte".
    private boolean list(String[] campi, OutputStream out) throws IOException, RichiestaMalformata {
        if (campi.length < 2) {
            throw new RichiestaMalformata();
        }
        String nome = campi[1].equals(Protocol.NONE) ? null : campi[1];
 
        // Le righe arrivano gia' nel formato: "temp_bo peer0 peer1"; qui non si formatta nulla, perche' la forma leggibile serve solo alle console.
        List<String> righe = stato.elencoRisorse(nome);
        scriviElenco(out, righe);
        return true;
    }
 
    private boolean peers(OutputStream out) throws IOException {
        List<String> attivi = stato.elencoPeerAttivi();
        scriviElenco(out, attivi);
        return true;
    }
 
    private boolean added(String[] campi, OutputStream out) throws IOException, RichiestaMalformata {
        if (campi.length < 2) {
            throw new RichiestaMalformata();
        }
        stato.aggiungiRisorsa(peerId, campi[1]);
        Protocol.writeLine(out, Protocol.RESP_OK);
        return true;
    }
 
    private boolean removed(String[] campi, OutputStream out) throws IOException, RichiestaMalformata {
        if (campi.length < 2) {
            throw new RichiestaMalformata();
        }
        stato.rimuoviRisorsa(peerId, campi[1]);
        Protocol.writeLine(out, Protocol.RESP_OK);
        return true;
    }
 
    /**
     * RESOLVE risorsa token|- esclusiCsv|- : sceglie il nodo da cui scaricare e ne acquisisce il token.
     *
     * Il monitor della tabella viene rilasciato al ritorno di risolvi(), e SOLO DOPO si acquisisce il token. 
     * Se il token si acquisisse tenendo il monitor della tabella, un nodo messo in attesa bloccherebbe l'intera rete: nessun altro potrebbe fare nemmeno una
     * LIST, perche' AggregatorState e' in mutua esclusione totale.
     *
     * Fra la scelta del candidato e l'acquisizione del token la tabella puo' cambiare, e il candidato potrebbe non possedere piu' la risorsa: NON si ricontrolla. 
     * La FETCH fallira', il client mandera' RELEASE FAIL e il suo ciclo di retry chiedera' un altro candidato.
     */
    private boolean resolve(String[] campi, OutputStream out)
            throws IOException, InterruptedException, RichiestaMalformata {
 
        if (campi.length < 4) {
            throw new RichiestaMalformata();
        }
        String risorsa = campi[1];
        String tokenRicevuto = campi[2];
        String esclusiCsv = campi[3];
 
        // Caso residuo: con il ciclo di download previsto il client ha gia' rilasciato il token precedente e invia "-". 
        // Si gestisce lo stesso, perche' un token non rilasciato terrebbe bloccato un nodo sorgente per tutti.
        rilasciaSeMio(tokenRicevuto);
 
        AggregatorState.EsitoRisolvi esito = stato.risolvi(peerId, risorsa, esclusi(esclusiCsv));
 
        if (esito.peerId == null) {
            // Nessun candidato: la riga di log si scrive comunque, perche' anche una richiesta andata a vuoto e' una richiesta di download e deve comparire nel registro.
            // Nessun token viene acquisito: non c'e' alcun nodo da bloccare.
            log.aggiungi(risorsa, null, peerId, DownloadLog.Esito.NON_DISPONIBILE);
            Protocol.writeLine(out, errore(Protocol.ERR_NOTFOUND));
            return true;
        }
 
        Protocol.writeLine(out, rispostaResolve(acquisisci(esito.peerId, risorsa), esito.peerId,
                esito.host, esito.porta));
        return true;
    }
 
    /**
     * RESOLVE_AT risorsa peerDestinazione token|- : download mirato su un nodo scelto dall'utente, quindi senza scelta del candidato e senza eviction.
     *
     * La verifica precede sempre l'acquisizione del token, come nella RESOLVE: acquisire il token di un peer per poi rispondere ERR NOTFOUND bloccherebbe quel nodo per una
     * richiesta che non ha nemmeno un indirizzo da proporre.
     */
    private boolean resolveAt(String[] campi, OutputStream out)
            throws IOException, InterruptedException, RichiestaMalformata {
 
        if (campi.length < 4) {
            throw new RichiestaMalformata();
        }
        String risorsa = campi[1];
        String destinazione = campi[2];
        String tokenRicevuto = campi[3];
 
        rilasciaSeMio(tokenRicevuto);
 
        AggregatorState.PeerInfo info = stato.indirizzoDi(destinazione);
        boolean utilizzabile = info != null && info.attivo && possiede(destinazione, risorsa);
 
        if (!utilizzabile) {
            // Stessa voce di log della RESOLVE senza candidati: senza di essa un download mirato fallito in risoluzione non produrrebbe alcuna RELEASE e sparirebbe dal registro.
            log.aggiungi(risorsa, null, peerId, DownloadLog.Esito.NON_DISPONIBILE);
            Protocol.writeLine(out, errore(Protocol.ERR_NOTFOUND));
            return true;
        }
 
        Protocol.writeLine(out, rispostaResolve(acquisisci(destinazione, risorsa), destinazione,
                info.host, info.porta));
        return true;
    }
 
    /**
     * RELEASE token OK|FAIL risorsa peerSorgente|- : chiude il download, aggiorna la tabella, scrive la riga di log e libera il nodo sorgente.
     *
     * Il token si rilascia per ULTIMO, dopo tabella e log. Un thread risvegliato dal rilascio va subito a scegliere un candidato: 
     * se lo svegliassimo prima di aver aggiornato la tabella, sceglierebbe guardando uno stato che sappiamo gia' superato.
     */
    private boolean release(String[] campi, OutputStream out) throws IOException, RichiestaMalformata {
        if (campi.length < 5) {
            throw new RichiestaMalformata();
        }
        String tokenRicevuto = campi[1];
        String esitoDichiarato = campi[2];
        String sorgenteDichiarata = campi[4];
 
        // La validazione dell'esito sta prima di ogni effetto: se fosse dentro il dispatch, un esito scritto male uscirebbe con ERR BADREQUEST dopo aver gia' letto la
        // sessione e lasciando il token appeso.
        boolean riuscito = esitoDichiarato.equals(Protocol.OUTCOME_OK);
        if (!riuscito && !esitoDichiarato.equals(Protocol.OUTCOME_FAIL)) {
            throw new RichiestaMalformata();
        }
 
        TokenManager.Sessione sessione = token.leggi(tokenRicevuto);
        if (sessione == null) {
            // Token sconosciuto o gia' chiuso: non si tocca ne' la tabella ne' il log. 
            // Il caso tipico e' la sorgente morta, per cui rilasciaTuttiDi ha gia' chiuso la sessione e scritto la voce FALLITO: scriverne un'altra qui la duplicherebbe.
            Protocol.writeLine(out, errore(Protocol.ERR_BADTOKEN));
            return true;
        }
        if (!sessione.richiedente.equals(peerId)) {
            // Il token esiste ma appartiene a un ALTRO nodo. Senza questo controllo un client potrebbe liberare il nodo sorgente mentre il vero titolare sta ancora
            // scaricando, facendo entrare un secondo download sullo stesso nodo. In piu' il download verrebbe attribuito al nodo sbagliato, sia nella tabella sia nel log. 
            Protocol.writeLine(out, errore(Protocol.ERR_BADTOKEN));
            return true;
        }
 
        if (riuscito) {
            // La RELEASE con esito OK vale anche come notifica di possesso: il richiedente ora ha la risorsa e la tabella lo registra qui, senza attendere un ADDED. 
            // Un secondo scambio aprirebbe una finestra in cui il nodo possiede la rilevazione e la tabella non lo sa, e se il nodo morisse li' dentro resterebbe disallineata.
            stato.aggiungiRisorsa(peerId, sessione.risorsa);
            log.aggiungi(sessione.risorsa, sessione.sorgente, peerId, DownloadLog.Esito.OK);
        } else if (!sorgenteDichiarata.equals(Protocol.NONE)) {
            stato.rimuoviRisorsa(sessione.sorgente, sessione.risorsa);
            log.aggiungi(sessione.risorsa, sessione.sorgente, peerId, DownloadLog.Esito.FALLITO);
        }
        // Fallimento con sorgente "-": nessuna scrittura, ne' tabella ne' log.
 
        token.rilascia(tokenRicevuto);
        Protocol.writeLine(out, Protocol.RESP_OK);
        return true;
    }
 
    /**
     * Pulizia eseguita su OGNI percorso di uscita. 
     * Sta in un finally e non sul solo ramo in cui readLine ritorna null per una ragione precisa: se il client muore mentre questo thread e' fermo
     * nella wait() di token.acquisisci, il thread non vede alcun EOF, perche' non sta leggendo il socket. 
     * Si risveglia quando il token si libera, lo acquisisce per un nodo ormai morto, e scopre la morte solo scrivendo la risposta, con una IOException. 
     * Se la pulizia non fosse qui, quel token resterebbe assegnato a un nodo inesistente e il nodo sorgente diventerebbe inaccessibile per sempre.
     */
    private void pulizia() {
        if (peerId != null) {
            // I download interrotti dalla scomparsa di questo nodo, sia come richiedente sia come sorgente.
            List<TokenManager.Sessione> chiuse = token.rilasciaTuttiDi(peerId);
            for (TokenManager.Sessione s : chiuse) {
                log.aggiungi(s.risorsa, s.sorgente, s.richiedente, DownloadLog.Esito.FALLITO);
            }
 
            // Il peer diventa inattivo ma le sue risorse restano in tabella.
            stato.disconnetti(peerId);
            System.out.println("Disconnesso " + peerId);
        }
 
        chiudi();
        aggregatore.rimuoviHandler(this);
    }
 
    /**
     * Chiude il socket. Lo chiama anche arresta() di Aggregator sul quit, da un altro thread.
     */
    public void chiudi() {
        try {
            socket.close();
        } catch (IOException e) {
            // Gia' chiuso: non c'e' nulla da fare e non c'e' nulla da segnalare.
        }
    }
 
    /**
     * Acquisizione del token con le stampe diagnostiche che rendono visibile il Meccanismo 3.
     *
     * Non si puo' sapere PRIMA se il token e' occupato senza aggiungere un metodo a
     * TokenManager, e comunque la risposta sarebbe gia' vecchia un istante dopo. Si misura
     * invece quanto e' durata l'acquisizione: se il thread e' rimasto nella wait() per piu' di
     * un attimo, il token era occupato e la stampa lo dice, con il tempo di attesa. E' cio'
     * che in demo distingue chi e' passato subito da chi ha aspettato.
     *
     * System.out e' gia' sincronizzato internamente: una println non si mescola con quella di
     * un altro thread.
     */
    private String acquisisci(String sorgente, String risorsa) throws InterruptedException {
        System.out.println(peerId + " chiede il token di " + sorgente + " per " + risorsa);
        long inizio = System.currentTimeMillis();
        String assegnato = token.acquisisci(sorgente, peerId, risorsa);
        long attesa = System.currentTimeMillis() - inizio;
        if (attesa > 100) {
            System.out.println(peerId + " ottiene " + assegnato + " su " + sorgente
                    + " dopo " + (attesa / 1000.0) + " s di attesa");
        } else {
            System.out.println(peerId + " ottiene " + assegnato + " su " + sorgente);
        }
        return assegnato;
    }
 
    /**
     * Rilascia un token ricevuto in una RESOLVE o RESOLVE_AT, ma SOLO se appartiene a questo nodo. 
     * Stesso motivo del controllo nella RELEASE: un nodo non deve poter liberare il token che un altro sta ancora usando. 
     */
    private void rilasciaSeMio(String tokenRicevuto) {
        if (tokenRicevuto.equals(Protocol.NONE)) {
            return;
        }
        TokenManager.Sessione sessione = token.leggi(tokenRicevuto);
        if (sessione != null && sessione.richiedente.equals(peerId)) {
            token.rilascia(tokenRicevuto);
        }
    }
 
    private static String rispostaResolve(String tokenAssegnato, String sorgente, String host, int porta) {
        return Protocol.RESP_OK + " " + tokenAssegnato + " " + sorgente + " " + host + " " + porta;
    }
 
    /**
     * AggregatorState non ha un metodo che risponda a "questo peer possiede questa risorsa?"
     * La si ricava dalla riga che elencoRisorse restituisce per quella risorsa, dove i campi dal secondo in poi sono i possessori.
     */
    private boolean possiede(String peerIdCercato, String risorsa) {
        List<String> righe = stato.elencoRisorse(risorsa);
        if (righe.isEmpty()) {
            return false;
        }
        String[] campi = Protocol.split(righe.get(0));
        for (int i = 1; i < campi.length; i++) {
            if (campi[i].equals(peerIdCercato)) {
                return true;
            }
        }
        return false;
    }
 
    // "-" significa nessun escluso. Il client rimanda ogni volta la lista completa, perche' l'aggregatore non tiene alcuno stato fra una RESOLVE e la successiva.
    private static List<String> esclusi(String csv) {
        List<String> elenco = new ArrayList<>();
        if (csv.equals(Protocol.NONE)) {
            return elenco;
        }
        for (String parte : csv.split(",")) {
            String peer = parte.trim();
            if (!peer.isEmpty()) {
                elenco.add(peer);
            }
        }
        return elenco;
    }
 
    private void scriviElenco(OutputStream out, List<String> righe) throws IOException {
        // Il conteggio precede le righe: chi legge sa quante righe consumare senza cercare un terminatore, che prima o poi comparirebbe dentro un dato.
        Protocol.writeLine(out, Protocol.RESP_OK + " " + righe.size());
        for (String riga : righe) {
            Protocol.writeLine(out, riga);
        }
    }
 
    // Trasforma l'IOException di formato in RichiestaMalformata.
    private static int numero(String campo) throws RichiestaMalformata {
        try {
            return Protocol.parseInt(campo);
        } catch (IOException e) {
            throw new RichiestaMalformata();
        }
    }
 
    private static String errore(String codice) {
        return Protocol.RESP_ERR + " " + codice;
    }
}