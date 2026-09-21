import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * AggregatorState = MECCANISMO 1 del progetto.
 *
 * Questa classe tiene lo stato "vero" dell'aggregatore:
 * - chi possiede quale risorsa (la tabella "risorsa -> lista di peerId")
 * - l'anagrafica dei peer registrati (peerId -> host/porta/attivo)
 * - il contatore che genera peer0, peer1, peer2, ...
 *
 * REGOLA 1:
 * TUTTI i metodi pubblici sono "synchronized" sullo STESSO monitor (l'istanza stessa di
 * AggregatorState, ce n'e' una sola nell'aggregatore).
 *
 * Perche' basta questo per rispettare la specifica "ogni altra richiesta deve attendere finche'
 * la tabella non e' di nuovo disponibile"?
 * Perche' in Java un thread dentro un metodo synchronized di un oggetto tiene il monitor di
 * quell'oggetto, e nessun altro thread puo' entrare in nessun metodo synchronized dello stesso
 * oggetto, nemmeno uno di sola lettura. Quindi, mentre un thread sta scrivendo, tutti gli altri
 * restano in coda ad aspettare il monitor: e' una mutua esclusione totale, non un lock
 * "scritture vs scritture" con letture libere. Quello sarebbe un ReadWriteLock, escluso dalle
 * regole di progetto e comunque inadatto a questa specifica, perche' permetterebbe di leggere la
 * tabella mentre qualcuno la sta ancora aggiornando.
 *
 * REGOLA 2:
 * I metodi di questa classe fanno solo lavoro in memoria: leggono/scrivono le HashMap e
 * ritornano. Non chiamano MAI un metodo di un altro oggetto synchronized (es. non chiamano
 * DownloadLog o TokenManager) e non fanno mai I/O di rete (non scrivono su un socket).
 * Chi orchestra le cose in sequenza (prima la tabella, poi il log, poi il socket) e' il thread
 * NodeHandler, non questa classe. Questo e' cio' che garantisce che un thread non tenga mai due
 * monitor insieme, e che nessun metodo di questa classe possa "restare bloccato a lungo": ogni
 * chiamata dura microsecondi.
 */
public class AggregatorState {

    /**
     * Anagrafica di un peer registrato. E' una "static class" annidata qui (non un file a se')
     * perche' e' un dato che esiste solo al servizio della tabella: chi possiede la tabella
     * possiede anche il suo tipo di supporto.
     */
    public static class PeerInfo {
        public final String peerId;
        public final String host;
        public final int porta;
        public boolean attivo;

        PeerInfo(String peerId, String host, int porta) {
            this.peerId = peerId;
            this.host = host;
            this.porta = porta;
            this.attivo = true;
        }

        // Copia difensiva: chi riceve un PeerInfo da fuori non deve poter modificare
        // l'originale custodito dentro il monitor.
        PeerInfo copia() {
            PeerInfo p = new PeerInfo(peerId, host, porta);
            p.attivo = attivo;
            return p;
        }
    }

    /**
     * Risultato di risolvi(): un piccolo oggetto valore con SOLO dati, senza comportamento.
     * Serve a far uscire dal monitor l'esito della scelta del candidato, cosi' il chiamante
     * (NodeHandler) puo' usarlo FUORI dal monitor della tabella (principio delle classi passive).
     *
     * peerId == null significa "nessun candidato trovato": il chiamante in quel caso NON deve
     * acquisire alcun token (vedi TokenManager).
     */
    public static class EsitoRisolvi {
        public final String peerId; // null se non c'e' candidato
        public final String host;
        public final int porta;

        EsitoRisolvi(String peerId, String host, int porta) {
            this.peerId = peerId;
            this.host = host;
            this.porta = porta;
        }

        static EsitoRisolvi nessunCandidato() {
            return new EsitoRisolvi(null, null, 0);
        }
    }

    // STATO PROTETTO DAL MONITOR (tre campi privati)

    /** risorsa -> lista dei peerId che la possiedono */
    private final Map<String, ArrayList<String>> possessori = new HashMap<>();

    /** peerId -> anagrafica del peer */
    private final Map<String, PeerInfo> registro = new HashMap<>();

    /** Prossimo numero da usare per generare "peerN". Cresce sempre, mai riusato. */
    private int prossimoId = 0;

    // REGISTRAZIONE: e' una singola scrittura atomica

    /**
     * Registra un nuovo nodo e le sue risorse in un'unica operazione synchronized. Chi chiama
     * (il NodeHandler) deve aver gia' letto tutte le righe della REGISTER PRIMA di chiamare
     * questo metodo: qui non si fa alcuna lettura dal socket, solo scrittura in memoria. E'
     * questo che rende impossibile una "registrazione a meta'" visibile da altri thread.
     */
    public synchronized String completaRegistrazione(String host, int porta, List<String> nomiRisorse) {
        String peerId = "peer" + prossimoId;
        prossimoId++;

        registro.put(peerId, new PeerInfo(peerId, host, porta));

        for (String risorsa : nomiRisorse) {
            ArrayList<String> lista = possessori.computeIfAbsent(risorsa, r -> new ArrayList<>());
            // Stesso controllo di aggiungiRisorsa: un nome ripetuto nella REGISTER non deve
            // comparire due volte fra i possessori della stessa risorsa.
            if (!lista.contains(peerId)) {
                lista.add(peerId);
            }
        }

        return peerId;
    }

    // SCRITTURE SINGOLE

    public synchronized void aggiungiRisorsa(String peerId, String risorsa) {
        ArrayList<String> lista = possessori.computeIfAbsent(risorsa, r -> new ArrayList<>());
        if (!lista.contains(peerId)) {
            lista.add(peerId);
        }
    }

    public synchronized void rimuoviRisorsa(String peerId, String risorsa) {
        ArrayList<String> lista = possessori.get(risorsa);
        if (lista != null) {
            lista.remove(peerId);
            // Non lasciamo in giro liste vuote: non e' obbligatorio, ma tiene la tabella
            // pulita e semplifica elencoRisorse().
            if (lista.isEmpty()) {
                possessori.remove(risorsa);
            }
        }
    }

    /**
     * Marca il peer come inattivo (crash o quit). NON tocca le sue risorse: restano nella
     * tabella, come richiesto dalla specifica "le rilevazioni non vengono eliminate, ma non
     * saranno piu' accessibili".
     */
    public synchronized void disconnetti(String peerId) {
        PeerInfo info = registro.get(peerId);
        if (info != null) {
            info.attivo = false;
        }
    }

    // LETTURE (tutte ritornano COPIE: nessun riferimento interno esce dal monitor)

    /**
     * nome == null -> tutte le risorse, ordinate alfabeticamente;
     * nome != null -> la sola riga di quella risorsa (lista vuota se nessuno la possiede).
     *
     * Il filtro sta dentro questo stesso metodo synchronized: niente metodo in piu', niente
     * copia dell'intera tabella quando basta una riga sola.
     *
     * Ogni riga e' nel FORMATO DEL PROTOCOLLO (sezione 5.2 del piano), cioe' campi separati da
     * spazio: "temp_bo peer0 peer1". Il NodeHandler la scrive cosi' com'e' sul socket come
     * risposta alla LIST. La forma leggibile "- temp_bo: peer0, peer1" (sezione 5.6) NON si
     * produce qui: e' presentazione, e la fanno le due console, ClientConsole e
     * AggregatorConsole. Questa classe custodisce i dati, non decide come mostrarli.
     *
     * I possessori sono ordinati per SUFFISSO NUMERICO del peerId (peer2 prima di peer10),
     * non alfabeticamente.
     */
    public synchronized List<String> elencoRisorse(String nome) {
        List<String> righe = new ArrayList<>();

        if (nome != null) {
            ArrayList<String> lista = possessori.get(nome);
            if (lista != null && !lista.isEmpty()) {
                righe.add(formattaRiga(nome, lista));
            }
            return righe; // 0 o 1 riga
        }

        List<String> nomiOrdinati = new ArrayList<>(possessori.keySet());
        Collections.sort(nomiOrdinati);
        for (String risorsa : nomiOrdinati) {
            righe.add(formattaRiga(risorsa, possessori.get(risorsa)));
        }
        return righe;
    }

    // Una riga nel formato del protocollo: "<risorsa> <peer> <peer> ...".
    private String formattaRiga(String risorsa, List<String> possessoriRisorsa) {
        List<String> copia = new ArrayList<>(possessoriRisorsa);
        copia.sort(Comparator.comparingInt(AggregatorState::suffissoNumerico));
        StringBuilder sb = new StringBuilder(risorsa);
        for (String peerId : copia) {
            sb.append(' ').append(peerId);
        }
        return sb.toString();
    }

    // Solo i peer attivi, ordinati per suffisso numerico del peerId.
    public synchronized List<String> elencoPeerAttivi() {
        List<String> attivi = new ArrayList<>();
        for (PeerInfo info : registro.values()) {
            if (info.attivo) {
                attivi.add(info.peerId);
            }
        }
        attivi.sort(Comparator.comparingInt(AggregatorState::suffissoNumerico));
        return attivi;
    }

    // Anagrafica di un peer (copia), o null se sconosciuto.
    public synchronized PeerInfo indirizzoDi(String peerId) {
        PeerInfo info = registro.get(peerId);
        return (info == null) ? null : info.copia();
    }

    /**
     * MECCANISMO 1 + scelta del candidato per il download (usata da RESOLVE).
     *
     * Fa, TUTTO sotto lo stesso monitor:
     * 1. rimuove dalla tabella le entry (peer, risorsa) che il chiamante ha gia' escluso
     *    (tentativi falliti in precedenza) e quelle dei peer ormai inattivi incontrate per la
     *    risorsa cercata;
     * 2. fra i possessori rimasti, esclude il richiedente stesso (non ha senso scaricare da se
     *    stessi);
     * 3. sceglie, fra quelli che restano, il peerId con il suffisso numerico piu' basso
     *    (scelta deterministica).
     *
     * esclusi puo' essere null o vuota: entrambi significano "nessun peer escluso", che sul
     * filo corrisponde al "-" della RESOLVE. Un null non gestito qui produrrebbe una
     * NullPointerException, e siccome questo metodo gira dentro un NodeHandler, uccide il
     * thread che serve quel nodo.
     *
     * Il risultato esce dal monitor come EsitoRisolvi (solo dati): l'acquisizione del token e
     * la scrittura sul log avvengono DOPO, fuori da questo metodo, nel NodeHandler.
     */
    public synchronized EsitoRisolvi risolvi(String richiedente, String risorsa, List<String> esclusi) {
        List<String> daEscludere = (esclusi == null) ? Collections.emptyList() : esclusi;

        ArrayList<String> lista = possessori.get(risorsa);
        if (lista == null || lista.isEmpty()) {
            return EsitoRisolvi.nessunCandidato();
        }

        // Eviction: rimuoviamo dalla lista vera (non da una copia) i possessori esclusi
        // esplicitamente e quelli ormai inattivi.
        lista.removeIf(peerId -> {
            if (daEscludere.contains(peerId)) {
                return true;
            }
            PeerInfo info = registro.get(peerId);
            return info == null || !info.attivo;
        });
        if (lista.isEmpty()) {
            possessori.remove(risorsa);
            return EsitoRisolvi.nessunCandidato();
        }

        // Fra i rimanenti, candidati validi = diversi dal richiedente.
        String scelto = null;
        for (String peerId : lista) {
            if (peerId.equals(richiedente)) {
                continue;
            }
            if (scelto == null || suffissoNumerico(peerId) < suffissoNumerico(scelto)) {
                scelto = peerId;
            }
        }
        if (scelto == null) {
            return EsitoRisolvi.nessunCandidato();
        }

        PeerInfo info = registro.get(scelto);
        return new EsitoRisolvi(scelto, info.host, info.porta);
    }

    // Per ordinare per suffisso numerico: "peer10" -> 10, cosi' peer2 viene prima di peer10.
    private static int suffissoNumerico(String peerId) {
        String cifre = peerId.replaceAll("[^0-9]", "");
        return cifre.isEmpty() ? 0 : Integer.parseInt(cifre);
    }
}
