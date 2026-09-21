import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * TokenManager = MECCANISMO 3 del progetto.
 *
 * Il token NON e' un identificativo di sessione, e' un LOCK DI ACCESSO al
 * nodo SORGENTE di un download. Esiste un token per ogni peer che possa
 * fare da sorgente: se peer1 e peer2 vogliono entrambi scaricare da peer0,
 * uno ottiene il token di peer0 e procede, l'altro ATTENDE (mai un errore)
 * finche' non si libera.
 *
 * Il lock e' per nodo SORGENTE, non globale: un download da peer0 e uno da
 * peer3 vanno in parallelo, perche' la condizione di attesa e' la chiave
 * del peer nella mappa, non lo stato dell'intero oggetto.
 *
 * REGOLA 1: tutti i metodi pubblici sono synchronized sullo stesso monitor
 * (questa istanza). A differenza di AggregatorState, qui un thread puo'
 * ADDORMENTARSI in acquisisci() con wait(): e' innocuo, perche' wait()
 * rilascia il monitor e non blocca gli altri metodi — incluso rilascia(),
 * che e' cio' che risveglia chi attende.
 *
 * IMPORTANTE: un thread in attesa qui non tiene MAI anche il monitor di
 * AggregatorState — quello e' gia' stato rilasciato prima di chiamare
 * acquisisci(). Se TokenManager fosse dentro AggregatorState, un'attesa
 * bloccherebbe l'intera tabella (nemmeno una "listdata" potrebbe passare).
 * Per questo sono due classi distinte.
 */
public class TokenManager {

    /**
     * Una sessione di download aperta: chi ha chiesto il token (richiedente),
     * per accedere a quale nodo (sorgente), per quale risorsa, e da quando.
     * Solo dati, nessun comportamento: serve al log e a rilasciaTuttiDi().
     */
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

    //STATO PROTETTO DAL MONITOR
    /**
     * peerSorgente -> peerId che attualmente tiene il suo token.
     * Chiave ASSENTE = token libero. E' la mappa su cui si aspetta.
     *
     * Per decidere se attendere basta sapere SE la chiave c'e' (containsKey):
     * il valore, cioe' CHI tiene il token, non viene letto dalla logica. Lo
     * teniamo comunque perche' in fase di debug permette di rispondere alla
     * domanda "chi sta occupando peer0 in questo momento?".
     */
    private final Map<String, String> titolarePerPeer = new HashMap<>();

    /** token (es. "t7") -> Sessione aperta con quel token. */
    private final Map<String, Sessione> sessioni = new HashMap<>();

    /** Contatore per generare token nella forma "t<N>", mai riusato. */
    private long prossimoToken = 0;

    /**
     * Prova ad acquisire il token del nodo peerSorgente per conto di
     * peerRichiedente. Se il token e' gia' occupato, il thread chiamante si
     * ADDORMENTA qui dentro finche' non si libera: NON viene mai restituito
     * un errore di "occupato", come richiesto dalla specifica (A.2) e
     * confermato dal tutor.
     *
     * Se l'attesa viene interrotta, l'InterruptedException esce da qui PRIMA
     * che il token sia stato assegnato: il chiamante non possiede nulla e
     * quindi non deve rilasciare nulla.
     *
     * @return il token assegnato (es. "t7"), da restituire poi a rilascia()
     */
    public synchronized String acquisisci(String peerSorgente, String peerRichiedente, String risorsa)
            throws InterruptedException {

        // SEMPRE while, MAI if: quando questo thread si risveglia deve
        // ricontrollare la condizione da capo, perche' fra la notifyAll() e
        // la riacquisizione EFFETTIVA del monitor da parte di QUESTO thread,
        // un altro thread risvegliato prima di lui puo' essersi gia' preso
        // il token appena liberato. Con un "if" quel controllo non verrebbe
        // rifatto e due thread potrebbero credersi entrambi titolari.
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
     * Rilascia un token e chiude la sua sessione, riuscita o fallita che sia
     * (chi chiama decide cosa scrivere nel log, questa classe non lo sa).
     * Risveglia tutti i thread in attesa di un token QUALSIASI (notifyAll):
     * ognuno ricontrollera' nel proprio "while" se il token che stava
     * aspettando (quello del peer giusto) e' davvero libero.
     *
     * @return la Sessione chiusa, o null se il token era gia' sconosciuto
     *         (gia' rilasciato, o mai esistito: capita se arriva una
     *         RELEASE duplicata o tardiva, e non deve mai far crashare nulla).
     *         Il caso tipico e' la sorgente morta: rilasciaTuttiDi() ha gia'
     *         chiuso la sessione, e la RELEASE del richiedente arriva dopo.
     */
    public synchronized Sessione rilascia(String token) {
        Sessione s = sessioni.remove(token);
        if (s == null) {
            return null; // token sconosciuto o gia' chiuso: nessun effetto
        }

        titolarePerPeer.remove(s.sorgente);

        // SEMPRE notifyAll, MAI notify: su questo monitor possono dormire
        // thread che aspettano token DI PEER DIVERSI. notify() ne
        // sveglierebbe uno a caso: se svegliasse uno che aspetta il token
        // di un altro peer (non quello appena liberato), quel thread
        // tornerebbe subito a dormire nel while, e il thread che INVECE
        // poteva procedere resterebbe addormentato per sempre, perche'
        // nessun altro evento lo sveglierebbe piu'. Con notifyAll() si
        // svegliano tutti, ognuno ricontrolla la propria condizione: chi
        // non puo' ancora procedere si riaddormenta (spreco accettabile),
        // ma nessuno resta perso.
        notifyAll();

        return s;
    }

    /**
    * Chiamato quando un nodo si disconnette (EOF, DISCONNECT o crash): libera
    * TUTTI i token che riguardano quel peer, in entrambi i ruoli possibili:
    * a. i token che teneva come RICHIEDENTE (era lui ad aver ottenuto
    *    l'accesso a un altro nodo, ed e' morto a meta');
    * b. l'eventuale token DEL peer stesso come SORGENTE (qualcun altro lo
    *    stava tenendo per scaricare da lui, e ora quel nodo non esiste piu').
    *
    * Senza (b) un nodo sorgente crashato resterebbe "prenotato" per sempre:
    * e' il caso limite piu' importante del Meccanismo 3.
    *
    * Una sessione ha un solo richiedente e un solo sorgente: basta un unico
    * passaggio su "sessioni" controllando entrambi i campi.
    *
    * Le sessioni chiuse vengono RESTITUITE perche' il NodeHandler scriva una
    * voce FALLITO nel log per ciascuna: questa classe non tocca il log, per
    * non tenere due monitor insieme (stessa regola di AggregatorState).
    *
    * Limite da conoscere: un thread fermo in wait() dentro acquisisci() non
    * ha ancora una sessione, quindi non viene raggiunto da questo metodo. Se
    * il suo client muore mentre aspetta, il thread si risveglia solo quando
    * il token si libera, lo acquisisce per un nodo ormai morto, e se ne
    * accorge scrivendo sul socket. Per questo rilasciaTuttiDi() va invocato
    * su OGNI percorso di uscita del NodeHandler, non solo sull'EOF.
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
            // SEMPRE notifyAll, MAI notify: stesso motivo di rilascia().
            notifyAll();
        }

        return chiuse;
    }

    /** Sola lettura, per il log: non modifica nulla, non ha bisogno di attendere. */
    public synchronized Sessione leggi(String token) {
        return sessioni.get(token);
    }
}
