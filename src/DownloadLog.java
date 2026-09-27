import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Il registro delle richieste di download, stampato dal comando log dell'aggregatore.
 * E' una risorsa condivisa (scrivono i NodeHandler, legge la console), quindi i metodi sono
 * synchronized. Ha un monitor diverso dalla tabella: scrivere nel log non fa attendere una
 * listdata. Non serve wait(): non c'e' nessuna condizione su cui aspettare.
 */
public class DownloadLog {

    /** L'esito in coda alla riga. NON_DISPONIBILE: nessun nodo possedeva la rilevazione. */
    public enum Esito {
        OK,
        FALLITO,
        NON_DISPONIBILE
    }

    // Una riga del registro: privata perche' esiste solo al servizio del log.
    private static class Voce {
        final String orario;
        final String risorsa;
        final String sorgente;
        final String destinazione;
        final Esito esito;

        Voce(String orario, String risorsa, String sorgente, String destinazione, Esito esito) {
            this.orario = orario;
            this.risorsa = risorsa;
            this.sorgente = sorgente;
            this.destinazione = destinazione;
            this.esito = esito;
        }
    }

    /**
     * Con i secondi, perche' i tentativi di uno stesso download cadono nello stesso minuto.
     * Puo' essere condivisa da tutti i thread perche' DateTimeFormatter e' immutabile.
     */
    private static final DateTimeFormatter FORMATO_ORARIO = DateTimeFormatter.ofPattern("HH:mm:ss");

    private final List<Voce> voci = new ArrayList<>();

    /**
     * Registra una richiesta conclusa. L'orario si legge qui, sotto il monitor: cosi' l'ordine
     * delle righe coincide sempre con l'ordine degli orari. Sorgente null diventa "-".
     */
    public synchronized void aggiungi(String risorsa, String sorgente, String destinazione, Esito esito) {
        String orario = LocalTime.now().format(FORMATO_ORARIO);
        String sorgenteRiga = (sorgente == null) ? "-" : sorgente;
        voci.add(new Voce(orario, risorsa, sorgenteRiga, destinazione, esito));
    }

    /**
     * Restituisce una copia, perche' chi la scorre lo fa fuori dal monitor. Le righe non hanno
     * trattino ne' intestazione: quelli li aggiunge la console.
     */
    public synchronized List<String> righe() {
        List<String> risultato = new ArrayList<>();
        for (Voce v : voci) {
            risultato.add(v.orario + " " + v.risorsa
                    + " da: " + v.sorgente
                    + " a: " + v.destinazione
                    + " esito: " + v.esito);
        }
        return risultato;
    }
}