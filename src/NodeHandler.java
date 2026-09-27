import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Un thread per ogni nodo connesso: legge una richiesta, la esegue, risponde, e ricomincia.
 * Chiama le classi di stato una alla volta, in sequenza: e' questo che garantisce che un
 * thread non tenga mai due monitor insieme, ne' uno mentre scrive sul socket.
 * Ogni istanza e' usata da un solo thread, quindi non le serve un monitor proprio.
 */
public class NodeHandler implements Runnable {

    /**
     * Richiesta scritta male. Serve a distinguerla da un socket morto, che produce la stessa
     * IOException: la prima riceve ERR BADREQUEST e la connessione resta aperta, il secondo
     * fa uscire dal ciclo.
     */
    private static class RichiestaMalformata extends Exception {
    }

    private final Socket socket;
    private final AggregatorState stato;
    private final TokenManager token;
    private final DownloadLog log;
    private final Aggregator aggregatore;

    // Null finche' non arriva la REGISTER: cosi' si riconosce un nodo non ancora registrato.
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
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();
            ciclo(in, out);
        } catch (IOException e) {
            // La connessione non c'e' piu' (anche per una riga oltre gli 8 KB): non si risponde.
        } catch (InterruptedException e) {
            // Dalla wait() di token.acquisisci: si ripristina il flag, che catturarla azzera.
            Thread.currentThread().interrupt();
        } finally {
            pulizia();
        }
    }

    /**
     * Il ciclo di servizio. IOException e InterruptedException chiudono la connessione;
     * gli errori della richiesta diventano una risposta ERR e il ciclo prosegue.
     */
    private void ciclo(InputStream in, OutputStream out) throws IOException, InterruptedException {
        while (true) {
            String riga = Protocol.readLine(in);
            if (riga == null) {
                return; // chiusura pulita: la pulizia la fa il finally
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

    /** Esegue una richiesta. Restituisce false quando la connessione va chiusa. */
    private boolean esegui(String[] campi, InputStream in, OutputStream out)
            throws IOException, InterruptedException, RichiestaMalformata {

        String comando = campi[0];

        // Prima della REGISTER e' ammessa solo la REGISTER.
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
            return false; // la chiusura la fa il finally
        }

        throw new RichiestaMalformata();
    }

    /**
     * REGISTER porta n, seguita da n nomi. I nomi si leggono tutti PRIMA di toccare la tabella,
     * poi si registra con una sola chiamata: nessuno vede una registrazione a meta'.
     * L'host non lo dichiara il nodo: lo ricava l'aggregatore dalla connessione.
     */
    private boolean register(String[] campi, InputStream in, OutputStream out)
            throws IOException, RichiestaMalformata {

        if (peerId != null) {
            // Una seconda REGISTER cambierebbe identita' al nodo a meta' dialogo.
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
                // Il nodo e' sparito a meta': la tabella non e' stata toccata.
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

    // LIST nome oppure "-", che significa tutte le risorse.
    private boolean list(String[] campi, OutputStream out) throws IOException, RichiestaMalformata {
        if (campi.length < 2) {
            throw new RichiestaMalformata();
        }
        String nome = campi[1].equals(Protocol.NONE) ? null : campi[1];

        // Le righe sono gia' nel formato del protocollo: qui non si formatta nulla.
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
     * RESOLVE: sceglie il nodo da cui scaricare e ne prende il token. Prima si sceglie sotto il
     * monitor della tabella, e SOLO DOPO averlo lasciato si prende il token, che puo' far
     * attendere: attendere tenendo la tabella bloccherebbe l'intera rete. Se nel frattempo il
     * candidato perde la risorsa non si ricontrolla: ci pensera' il retry del client.
     */
    private boolean resolve(String[] campi, OutputStream out)
            throws IOException, InterruptedException, RichiestaMalformata {

        if (campi.length < 4) {
            throw new RichiestaMalformata();
        }
        String risorsa = campi[1];
        String tokenRicevuto = campi[2];
        String esclusiCsv = campi[3];

        // Di norma il client manda "-"; se manda un token suo, lo si libera.
        rilasciaSeMio(tokenRicevuto);

        AggregatorState.EsitoRisolvi esito = stato.risolvi(peerId, risorsa, esclusi(esclusiCsv));

        if (esito.peerId == null) {
            // Nessun candidato: nessun token da prendere, ma la richiesta va comunque nel log.
            log.aggiungi(risorsa, null, peerId, DownloadLog.Esito.NON_DISPONIBILE);
            Protocol.writeLine(out, errore(Protocol.ERR_NOTFOUND));
            return true;
        }

        Protocol.writeLine(out, rispostaResolve(acquisisci(esito.peerId, risorsa), esito.peerId,
                esito.host, esito.porta));
        return true;
    }

    /**
     * RESOLVE_AT: download da un nodo scelto dall'utente. Si verifica che il nodo sia attivo e
     * possieda la risorsa PRIMA di prendere il suo token, per non bloccarlo inutilmente.
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
            log.aggiungi(risorsa, null, peerId, DownloadLog.Esito.NON_DISPONIBILE);
            Protocol.writeLine(out, errore(Protocol.ERR_NOTFOUND));
            return true;
        }

        Protocol.writeLine(out, rispostaResolve(acquisisci(destinazione, risorsa), destinazione,
                info.host, info.porta));
        return true;
    }

    /**
     * RELEASE: chiude il download, aggiorna tabella e log, libera il nodo sorgente. Il token si
     * libera per ULTIMO: chi viene svegliato sceglie subito un candidato, e deve trovare la
     * tabella gia' aggiornata.
     */
    private boolean release(String[] campi, OutputStream out) throws IOException, RichiestaMalformata {
        if (campi.length < 5) {
            throw new RichiestaMalformata();
        }
        String tokenRicevuto = campi[1];
        String esitoDichiarato = campi[2];
        String sorgenteDichiarata = campi[4];

        // Si controlla l'esito prima di fare qualunque cosa.
        boolean riuscito = esitoDichiarato.equals(Protocol.OUTCOME_OK);
        if (!riuscito && !esitoDichiarato.equals(Protocol.OUTCOME_FAIL)) {
            throw new RichiestaMalformata();
        }

        TokenManager.Sessione sessione = token.leggi(tokenRicevuto);
        if (sessione == null) {
            // Token gia' chiuso, di solito perche' la sorgente e' morta e la pulizia ha gia'
            // scritto la voce FALLITO: non si scrive nulla, per non duplicarla.
            Protocol.writeLine(out, errore(Protocol.ERR_BADTOKEN));
            return true;
        }
        if (!sessione.richiedente.equals(peerId)) {
            // Il token e' di un altro nodo: liberarlo farebbe entrare un secondo download sullo
            // stesso nodo sorgente mentre il primo e' ancora in corso.
            Protocol.writeLine(out, errore(Protocol.ERR_BADTOKEN));
            return true;
        }

        if (riuscito) {
            // Vale anche come notifica di possesso: ora il richiedente ha la rilevazione.
            stato.aggiungiRisorsa(peerId, sessione.risorsa);
            log.aggiungi(sessione.risorsa, sessione.sorgente, peerId, DownloadLog.Esito.OK);
        } else if (!sorgenteDichiarata.equals(Protocol.NONE)) {
            stato.rimuoviRisorsa(sessione.sorgente, sessione.risorsa);
            log.aggiungi(sessione.risorsa, sessione.sorgente, peerId, DownloadLog.Esito.FALLITO);
        }
        // Fallimento con sorgente "-": nessuna scrittura.

        token.rilascia(tokenRicevuto);
        Protocol.writeLine(out, Protocol.RESP_OK);
        return true;
    }

    /**
     * Pulizia su OGNI uscita, per questo sta nel finally. Se il nodo muore mentre questo thread
     * aspetta un token, il thread non se ne accorge: si sveglia, prende il token e lo scopre solo
     * scrivendo la risposta. Senza questa pulizia quel token resterebbe bloccato per sempre.
     */
    private void pulizia() {
        if (peerId != null) {
            // I download interrotti dalla scomparsa di questo nodo, da richiedente o da sorgente.
            List<TokenManager.Sessione> chiuse = token.rilasciaTuttiDi(peerId);
            for (TokenManager.Sessione s : chiuse) {
                log.aggiungi(s.risorsa, s.sorgente, s.richiedente, DownloadLog.Esito.FALLITO);
            }

            // Il nodo diventa inattivo, ma le sue risorse restano in tabella.
            stato.disconnetti(peerId);
            System.out.println("Disconnesso " + peerId);
        }

        chiudi();
        aggregatore.rimuoviHandler(this);
    }

    /** Chiude il socket. La chiama anche arresta() di Aggregator sul quit. */
    public void chiudi() {
        try {
            socket.close();
        } catch (IOException e) {
            // Gia' chiuso.
        }
    }

    /**
     * Prende il token misurando quanto si e' aspettato: se l'attesa supera un attimo, il token
     * era occupato e la stampa lo dice. E' cio' che rende visibile il Meccanismo 3 in demo.
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

    /** Libera un token ricevuto solo se appartiene a questo nodo, come nella RELEASE. */
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

    // Se un nodo possiede una risorsa lo si ricava dalla riga di elencoRisorse per quella risorsa.
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

    // "-" significa nessun escluso. Il client rimanda ogni volta la lista completa.
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

    // Prima il numero di righe, poi le righe: chi legge sa quante consumarne.
    private void scriviElenco(OutputStream out, List<String> righe) throws IOException {
        Protocol.writeLine(out, Protocol.RESP_OK + " " + righe.size());
        for (String riga : righe) {
            Protocol.writeLine(out, riga);
        }
    }

    // Trasforma l'errore di formato del numero in RichiestaMalformata.
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