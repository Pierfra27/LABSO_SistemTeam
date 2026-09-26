import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Meccanismo 1: la tabella delle rilevazioni (chi possiede cosa) e l'anagrafica dei peer.
 * Tutti i metodi pubblici sono synchronized sullo stesso monitor: mentre uno scrive, anche le
 * letture attendono, come chiede la specifica. I metodi lavorano solo in memoria e non chiamano
 * altri oggetti sincronizzati, cosi' un thread non tiene mai due monitor insieme.
 */
public class AggregatorState {

    /** Anagrafica di un peer. Annidata perche' esiste solo al servizio della tabella. */
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

        // Copia difensiva: da fuori non si deve poter modificare l'originale.
        PeerInfo copia() {
            PeerInfo p = new PeerInfo(peerId, host, porta);
            p.attivo = attivo;
            return p;
        }
    }

    /**
     * Risultato di risolvi(): solo dati, da usare fuori dal monitor.
     * peerId null significa nessun candidato, e in quel caso non si acquisisce alcun token.
     */
    public static class EsitoRisolvi {
        public final String peerId;
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

    /**
     * Ordina i peerId per numero (peer2 prima di peer10), cosa che l'ordine alfabetico non fa.
     * Implementa Comparator come gli handler implementano Runnable. Non ha stato, quindi
     * un'unica istanza e' condivisa da tutti i thread senza sincronizzazione.
     */
    private static class OrdinePerSuffisso implements Comparator<String> {
        @Override
        public int compare(String a, String b) {
            return Integer.compare(suffissoNumerico(a), suffissoNumerico(b));
        }
    }

    private static final Comparator<String> PER_SUFFISSO = new OrdinePerSuffisso();

    // Stato protetto dal monitor.

    /** Per ogni risorsa, i peer che la possiedono. */
    private final Map<String, ArrayList<String>> possessori = new HashMap<>();

    /** Per ogni peerId, la sua anagrafica. */
    private final Map<String, PeerInfo> registro = new HashMap<>();

    /** Numero del prossimo peerId: cresce sempre e non viene mai riusato. */
    private int prossimoId = 0;

    /**
     * Registra un nodo e tutte le sue risorse in un'unica operazione atomica. Le righe della
     * REGISTER sono gia' state lette dal NodeHandler, quindi nessun altro thread puo' vedere
     * una registrazione a meta'.
     */
    public synchronized String completaRegistrazione(String host, int porta, List<String> nomiRisorse) {
        String peerId = "peer" + prossimoId;
        prossimoId++;

        registro.put(peerId, new PeerInfo(peerId, host, porta));

        for (String risorsa : nomiRisorse) {
            ArrayList<String> lista = listaPossessori(risorsa);
            // Stesso controllo di aggiungiRisorsa: un nome ripetuto non duplica il peer.
            if (!lista.contains(peerId)) {
                lista.add(peerId);
            }
        }

        return peerId;
    }

    /**
     * Restituisce la lista dei possessori di una risorsa, creandola se non esiste.
     * Non e' synchronized perche' e' privato e lo chiamano solo metodi che hanno gia' il monitor.
     */
    private ArrayList<String> listaPossessori(String risorsa) {
        ArrayList<String> lista = possessori.get(risorsa);
        if (lista == null) {
            lista = new ArrayList<>();
            possessori.put(risorsa, lista);
        }
        return lista;
    }

    public synchronized void aggiungiRisorsa(String peerId, String risorsa) {
        ArrayList<String> lista = listaPossessori(risorsa);
        if (!lista.contains(peerId)) {
            lista.add(peerId);
        }
    }

    public synchronized void rimuoviRisorsa(String peerId, String risorsa) {
        ArrayList<String> lista = possessori.get(risorsa);
        if (lista != null) {
            lista.remove(peerId);
            // Le liste vuote si eliminano per tenere pulita la tabella.
            if (lista.isEmpty()) {
                possessori.remove(risorsa);
            }
        }
    }

    /**
     * Segna il peer come inattivo. Le sue risorse restano in tabella ma non verranno piu'
     * proposte per un download, come chiede la specifica.
     */
    public synchronized void disconnetti(String peerId) {
        PeerInfo info = registro.get(peerId);
        if (info != null) {
            info.attivo = false;
        }
    }

    // Le letture restituiscono sempre copie: nessun riferimento interno esce dal monitor.

    /**
     * Con nome null restituisce tutte le risorse, altrimenti solo quella richiesta.
     * Le righe sono nel formato del protocollo ("temp_bo peer0 peer1"): la forma leggibile la
     * costruiscono le console, perche' questa classe custodisce i dati e non li presenta.
     */
    public synchronized List<String> elencoRisorse(String nome) {
        List<String> righe = new ArrayList<>();

        if (nome != null) {
            ArrayList<String> lista = possessori.get(nome);
            if (lista != null && !lista.isEmpty()) {
                righe.add(formattaRiga(nome, lista));
            }
            return righe;
        }

        List<String> nomiOrdinati = new ArrayList<>(possessori.keySet());
        Collections.sort(nomiOrdinati);
        for (String risorsa : nomiOrdinati) {
            righe.add(formattaRiga(risorsa, possessori.get(risorsa)));
        }
        return righe;
    }

    // Una riga nel formato del protocollo: la risorsa seguita dai suoi possessori.
    private String formattaRiga(String risorsa, List<String> possessoriRisorsa) {
        List<String> copia = new ArrayList<>(possessoriRisorsa);
        Collections.sort(copia, PER_SUFFISSO);
        StringBuilder sb = new StringBuilder(risorsa);
        for (String peerId : copia) {
            sb.append(' ').append(peerId);
        }
        return sb.toString();
    }

    // Solo i peer attivi, ordinati per numero.
    public synchronized List<String> elencoPeerAttivi() {
        List<String> attivi = new ArrayList<>();
        for (PeerInfo info : registro.values()) {
            if (info.attivo) {
                attivi.add(info.peerId);
            }
        }
        Collections.sort(attivi, PER_SUFFISSO);
        return attivi;
    }

    // Anagrafica di un peer (una copia), oppure null se sconosciuto.
    public synchronized PeerInfo indirizzoDi(String peerId) {
        PeerInfo info = registro.get(peerId);
        return (info == null) ? null : info.copia();
    }

    /**
     * Sceglie da quale peer scaricare: toglie dalla tabella gli esclusi e i peer inattivi,
     * scarta il richiedente e prende il peer con il numero piu' basso. Esclusi null equivale a
     * lista vuota. Token e log si gestiscono dopo, fuori dal monitor, nel NodeHandler.
     */
    public synchronized EsitoRisolvi risolvi(String richiedente, String risorsa, List<String> esclusi) {
        List<String> daEscludere = (esclusi == null) ? Collections.emptyList() : esclusi;

        ArrayList<String> lista = possessori.get(risorsa);
        if (lista == null || lista.isEmpty()) {
            return EsitoRisolvi.nessunCandidato();
        }

        // Si rimuove con l'Iterator: togliere elementi dentro un for-each solleverebbe una
        // ConcurrentModificationException.
        Iterator<String> it = lista.iterator();
        while (it.hasNext()) {
            String peerId = it.next();
            PeerInfo info = registro.get(peerId);
            boolean escluso = daEscludere.contains(peerId);
            boolean inattivo = (info == null) || !info.attivo;
            if (escluso || inattivo) {
                it.remove();
            }
        }
        if (lista.isEmpty()) {
            possessori.remove(risorsa);
            return EsitoRisolvi.nessunCandidato();
        }

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

    // Estrae il numero dal peerId: da "peer10" si ottiene 10.
    private static int suffissoNumerico(String peerId) {
        String cifre = peerId.replaceAll("[^0-9]", "");
        return cifre.isEmpty() ? 0 : Integer.parseInt(cifre);
    }
}

