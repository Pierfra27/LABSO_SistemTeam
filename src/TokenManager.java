import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * MECCANISMO 3: il token e' un lock di accesso al nodo sorgente di un download.
 * Un token per ogni peer: chi lo trova occupato ATTENDE (mai errore, come da tutor).
 * Token di peer diversi sono indipendenti, quindi i download vanno in parallelo.
 * Classe separata da AggregatorState: chi attende qui non blocca la tabella.
 */
public class TokenManager {

    /** Dati di un download aperto: chi, da quale nodo, cosa e da quando. Serve al log. */
    public static class Sessione {
        public final String richiedente;
        public final String sorgente;
        public final String risorsa;
        public final long apertaAlle; // System.currentTimeMillis()

        Sessione(String richiedente, String sorgente, String risorsa, long apertaAlle) {
            this.richiedente = richiedente;
            this.sorgente = sorgente;
            this.risorsa = risorsa;
            this.apertaAlle = apertaAlle;
        }
    }

    // STATO PROTETTO DAL MONITOR (tutti i metodi pubblici sono synchronized)

    /**
     * peerSorgente -> chi tiene il suo token. Chiave assente = token libero.
     * La logica guarda solo se la chiave c'e'; il valore serve per il debug.
     */
    private final Map<String, String> titolarePerPeer = new HashMap<>();

    /** token (es. "t7") -> Sessione aperta con quel token. */
    private final Map<String, Sessione> sessioni = new HashMap<>();

    /** Contatore per i token "t<N>", mai riusato. */
    private long prossimoToken = 0;

    /**
     * Acquisisce il token di peerSorgente; se e' occupato il thread dorme qui.
     * wait() rilascia il monitor, quindi rilascia() puo' entrare e svegliarlo.
     * Se l'attesa viene interrotta, nessun token e' stato assegnato.
     */
    public synchronized String acquisisci(String peerSorgente, String peerRichiedente, String risorsa)
            throws InterruptedException {

        // while e non if: al risveglio un altro thread puo' aver gia' preso
        // il token, quindi la condizione va ricontrollata ogni volta.
        while (titolarePerPeer.containsKey(peerSorgente)) {
            wait();
        }

        titolarePerPeer.put(peerSorgente, peerRichiedente);

        String token = "t" + prossimoToken;
        prossimoToken++;

        sessioni.put(token, new Sessione(peerRichiedente, peerSorgente, risorsa, System.currentTimeMillis()));

        return token;
    }

    /**
     * Rilascia il token e chiude la sessione (riuscita o fallita, decide il chiamante).
     * Ritorna null se il token era gia' chiuso, es. RELEASE arrivata dopo che
     * rilasciaTuttiDi() ha gia' liberato la sorgente morta: nessun crash.
     */
    public synchronized Sessione rilascia(String token) {
        Sessione s = sessioni.remove(token);
        if (s == null) {
            return null; // token sconosciuto o gia' chiuso: nessun effetto
        }

        titolarePerPeer.remove(s.sorgente);

        // notifyAll e non notify: qui dormono thread che aspettano token di peer
        // diversi; notify() potrebbe svegliare quello sbagliato e lasciare
        // addormentato per sempre chi poteva procedere.
        notifyAll();

        return s;
    }

    /**
     * Alla disconnessione di un peer libera tutti i suoi token, in entrambi i ruoli:
     * come richiedente (moriva a meta' download) e come sorgente (nodo sparito).
     * Restituisce le sessioni chiuse: il log FALLITO lo scrive il NodeHandler.
     * Non raggiunge chi e' ancora in wait(): va chiamato su OGNI uscita del NodeHandler.
     */
    public synchronized List<Sessione> rilasciaTuttiDi(String peerId) {
        List<Sessione> chiuse = new ArrayList<>();

        Iterator<Map.Entry<String, Sessione>> it = sessioni.entrySet().iterator();
        while (it.hasNext()) {
            Sessione s = it.next().getValue();
            boolean comeRichiedente = s.richiedente.equals(peerId);
            boolean comeSorgente = s.sorgente.equals(peerId);
            if (comeRichiedente || comeSorgente) {
                titolarePerPeer.remove(s.sorgente);
                chiuse.add(s);
                it.remove();
            }
        }

        if (!chiuse.isEmpty()) {
            notifyAll(); // stesso motivo di rilascia()
        }

        return chiuse;
    }

    /** Sola lettura, per il log. */
    public synchronized Sessione leggi(String token) {
        return sessioni.get(token);
    }
}
