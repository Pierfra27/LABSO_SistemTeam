import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * L'unica connessione verso l'aggregatore, aperta all'avvio e tenuta viva fino al quit.
 * E' l'unico punto del progetto in cui si tiene un monitor durante un'operazione di rete:
 * serve a rendere atomico lo scambio richiesta-risposta, altrimenti due thread potrebbero
 * leggere l'uno la risposta dell'altro. Il timeout di lettura evita di restare bloccati.
 */
public class AggregatorLink {

    // Per quasi tutti i messaggi una risposta che non arriva entro 10 secondi e' un'anomalia.
    private static final int TIMEOUT_NORMALE = 10_000;

    // Codice interno, non del protocollo: distingue "connessione persa" da un ERR vero.
    public static final String LINK_CADUTO = "LINKCADUTO";

    /** Il risultato di uno scambio: solo dati, da usare fuori dal monitor. */
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
     * Manda una richiesta e legge la risposta. attendeElenco va a true solo per LIST e PEERS,
     * le cui risposte sono seguite da un elenco di righe: lo dichiara il chiamante, che sa
     * quale comando ha mandato, invece di tirare a indovinare dalla risposta.
     */
    public synchronized Risposta invia(String richiesta, boolean attendeElenco) {
        return scambia(richiesta, new ArrayList<>(), attendeElenco);
    }

    /**
     * Manda una richiesta seguita da altre righe: serve alla REGISTER, che porta i nomi delle
     * rilevazioni. Le righe partono nello stesso scambio sincronizzato dell'intestazione, cosi'
     * nessun altro messaggio puo' infilarsi in mezzo.
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
                // L'aggregatore ha chiuso la connessione.
                linkCaduto = true;
                return caduto();
            }

            String[] campi = Protocol.split(primaRiga);
            boolean ok = campi.length > 0 && campi[0].equals(Protocol.RESP_OK);

            List<String> righeExtra = new ArrayList<>();
            if (ok && attendeElenco) {
                if (campi.length < 2) {
                    // Elenco senza conteggio: non si sa quante righe leggere, il dialogo e' perso.
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
            // Errore di rete o conteggio non valido: non si sa piu' a che punto sia lo stream,
            // quindi la connessione non si usa piu'.
            linkCaduto = true;
            return caduto();
        }
    }

    /**
     * Come invia(), ma senza timeout: si usa solo per RESOLVE e RESOLVE_AT, che possono restare
     * in attesa del token anche a lungo. Le specifiche vietano che quell'attesa finisca in
     * errore. Il timeout normale si ripristina nel finally, su ogni percorso di uscita.
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
                // Socket gia' rotto: l'errore l'ha gia' segnalato invia().
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