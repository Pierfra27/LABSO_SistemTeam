import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
 
/**
 * L'unica connessione verso l'aggregatore, aperta all'avvio del nodo e tenuta viva fino al
 * quit. E' persistente e non usa-e-getta perche' e' cosi' che l'aggregatore distingue una
 * chiusura pulita da un crash: se il socket cade senza DISCONNECT, lui riceve EOF e lo sa
 * subito. Con una connessione nuova a ogni comando non potrebbe accorgersene mai.
 *
 * QUESTO E' L'UNICO PUNTO IN CUI SI TIENE UN MONITOR DURANTE UN'OPERAZIONE DI RETE, 
 * ed e' un'eccezione voluta. invia() e' synchronized per l'INTERO scambio
 * richiesta-risposta, non solo per la scrittura: se due thread del nodo potessero
 * interlacciarsi sulla stessa connessione, uno leggerebbe la risposta destinata all'altro e
 * il dialogo si sfaserebbe per sempre. Tenere il monitor anche durante la lettura e' cio'
 * che rende lo scambio atomico.
 *
 * Le due mitigazioni obbligatorie di questa scelta:
 *  - timeout di lettura di 10 secondi come valore normale, cosi' un aggregatore morto
 *    produce un'eccezione gestibile invece di un blocco permanente;
 *  - il flag linkCaduto: dopo un errore di I/O la connessione si considera persa e ogni
 *    chiamata successiva fallisce subito, invece di riprovare. Il progetto non prevede
 *    alcun riconnessione automatica: meglio un errore chiaro che un nodo che sembra vivo.
 *
 * Regola che vincola i chiamanti: nessuno invoca invia() da dentro un blocco synchronized.
 * DownloadManager scrive nel LocalStore e SOLO DOPO, tornato fuori, manda la RELEASE.
 * Altrimenti si terrebbero due monitor insieme, uno dei quali durante l'I/O di rete.
 */
public class AggregatorLink {
 
    //Timeout di lettura normale: per quasi tutti i messaggi una mancata risposta e' un'anomalia.
    private static final int TIMEOUT_NORMALE = 10_000;
 
    //Codice interno, non del protocollo: distingue "la connessione non c'e' piu'" da un ERR vero.
    public static final String LINK_CADUTO = "LINKCADUTO";
 
    /**
     * Il risultato di uno scambio con l'aggregatore. Solo dati, nessun comportamento: stesso
     * criterio di EsitoRisolvi, cosi' chi lo riceve lo usa fuori dal monitor.
     */
    public static class Risposta {
        public final boolean ok;
        public final String[] campi;          // la prima riga, gia' spezzata
        public final List<String> righeExtra; // le righe seguenti, per LIST e PEERS
 
        Risposta(boolean ok, String[] campi, List<String> righeExtra) {
            this.ok = ok;
            this.campi = campi;
            this.righeExtra = righeExtra;
        }
    }
 
    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;
    private boolean linkCaduto = false;
 
    public AggregatorLink(Socket socket) throws IOException {
        this.socket = socket;
        this.in = socket.getInputStream();
        this.out = socket.getOutputStream();
        this.socket.setSoTimeout(TIMEOUT_NORMALE);
    }
 
    /**
     * Manda una richiesta e legge la risposta.
     *
     * @param attendeElenco true SOLO per i comandi la cui risposta e' "OK <n>" seguita da n
     *        righe, cioe' LIST e PEERS. Lo sa il chiamante, che ha appena scelto il comando,
     *        e per questo glielo si chiede invece di indovinarlo qui guardando se il secondo
     *        campo somiglia a un numero. Quell'indovinello funzionerebbe finche' token e
     *        peerId non sono numerici, ma il giorno in cui lo diventassero si leggerebbero
     *        righe che non esistono e OGNI risposta successiva risulterebbe sfasata di una
     *        riga, in modo definitivo: un guasto che non produce eccezioni.
     */
    public synchronized Risposta invia(String richiesta, boolean attendeElenco) {
        return scambia(richiesta, new ArrayList<>(), attendeElenco);
    }
 
    /**
     * Manda una richiesta seguita da altre righe, e legge una risposta semplice.
     *
     * Serve alla REGISTER, l'unico messaggio del protocollo con un corpo: l'intestazione
     * "REGISTER <porta> <n>" e subito dopo le n righe con i nomi delle rilevazioni. Le righe
     * vanno scritte DENTRO lo stesso scambio sincronizzato dell'intestazione: se fossero
     * inviate da una chiamata separata, un altro thread potrebbe infilare un proprio
     * messaggio fra l'intestazione e il corpo, e l'aggregatore leggerebbe quel messaggio come
     * se fosse il nome di una rilevazione.
     */
    public synchronized Risposta invia(String richiesta, List<String> righeSeguenti) {
        return scambia(richiesta, righeSeguenti, false);
    }
 
    private Risposta scambia(String richiesta, List<String> righeSeguenti, boolean attendeElenco) {
        if (linkCaduto) {
            return caduto();
        }
 
        try {
            Protocol.writeLine(out, richiesta);
            for (String riga : righeSeguenti) {
                Protocol.writeLine(out, riga);
            }
 
            String primaRiga = Protocol.readLine(in);
            if (primaRiga == null) {
                // L'aggregatore ha chiuso: non e' una risposta vuota, e' la fine del dialogo.
                linkCaduto = true;
                return caduto();
            }
 
            String[] campi = Protocol.split(primaRiga);
            boolean ok = campi.length > 0 && campi[0].equals(Protocol.RESP_OK);
 
            List<String> righeExtra = new ArrayList<>();
            if (ok && attendeElenco) {
                if (campi.length < 2) {
                    // Un elenco senza conteggio e' una risposta malformata: non si tira a
                    // indovinare quante righe leggere, si considera perso il dialogo.
                    linkCaduto = true;
                    return caduto();
                }
                int quante = Protocol.parseInt(campi[1]);
                for (int i = 0; i < quante; i++) {
                    String riga = Protocol.readLine(in);
                    if (riga == null) {
                        linkCaduto = true;
                        return caduto();
                    }
                    righeExtra.add(riga);
                }
            }
 
            return new Risposta(ok, campi, righeExtra);
 
        } catch (IOException e) {
            // Vale sia per un errore di rete sia per un conteggio non numerico dentro un
            // elenco: in entrambi i casi non si sa piu' a che punto sia lo stream, e l'unica
            // cosa sicura e' smettere di usarlo.
            linkCaduto = true;
            return caduto();
        }
    }
 
    /**
     * Come invia(), ma senza timeout di lettura. Va usata SOLO per RESOLVE e RESOLVE_AT.
     *
     * Con il token inteso come lock sul nodo sorgente (Meccanismo 3), una RESOLVE puo'
     * legittimamente non rispondere per decine di secondi, perche' il nodo da cui si vuole
     * scaricare e' occupato da un altro download. Con i 10 secondi normali il client si
     * arrenderebbe proprio mentre il sistema sta funzionando come previsto, e le specifiche
     * vietano che una richiesta in attesa termini con un errore.
     *
     * Nota sui monitor: questo metodo e' synchronized e chiama invia(), anch'esso
     * synchronized sullo stesso oggetto.
     */
    public synchronized Risposta inviaConAttesaIndefinita(String richiesta) {
        try {
            socket.setSoTimeout(0);
        } catch (IOException e) {
            linkCaduto = true;
            return caduto();
        }
 
        try {
            return invia(richiesta, false);
        } finally {
            try {
                socket.setSoTimeout(TIMEOUT_NORMALE);
            } catch (IOException e) {
                // Il socket e' gia' rotto: l'errore vero e' gia' stato segnalato da invia().
            }
        }
    }
 
    public synchronized boolean linkCaduto() {
        return linkCaduto;
    }
 
    private static Risposta caduto() {
        return new Risposta(false, new String[]{Protocol.RESP_ERR, LINK_CADUTO}, new ArrayList<>());
    }
}