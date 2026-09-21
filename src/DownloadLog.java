import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * E'il registro di TUTTE le richieste di download passate per l'aggregatore,
 * quello che il comando "log" della console dell'aggregatore stampa.
 *
 * E' una risorsa condivisa: ci scrivono in parallelo tutti i NodeHandler (uno per nodo sensore collegato, ognuno sul proprio thread) 
 * e la legge il thread della AggregatorConsole. Senza sincronizzazione due aggiunte simultanee potrebbero perdersi a vicenda, 
 * e una lettura concorrente a un'aggiunta potrebbe vedere la lista a meta' aggiornamento.
 *
 * Il monitor e' l'istanza di DownloadLog, quindi e' un monitor diverso da quello di AggregatorState. E' una scelta voluta: 
 * scrivere una riga di log non deve mai far attendere una "listdata", ne' viceversa. Se il log fosse un campo dentro AggregatorState
 * le due operazioni si contenderebbero lo stesso lock pur non toccando gli stessi dati.
 *
 * Non esiste alcuna condizione su cui un thread debba sospendersi. 
 * Un'aggiunta e una lettura si possono sempre eseguire subito, serve solo che non avvengano insieme: basta la mutua esclusione. 
 * E' la differenza con TokenManager, dove invece un thread deve attendere che il token del nodo destinatario venga rilasciato.
 */

public class DownloadLog {

    /**
     * L'esito di una richiesta di download, cosi' come compare in coda alla riga del log.
     * NON_DISPONIBILE e' il caso in cui nessun nodo possedeva la risorsa
     */

    public enum Esito {
        OK,
        FALLITO,
        NON_DISPONIBILE
    }

    // Una riga del registro. E' privata perche' esiste solo al servizio del log.

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
     * Formato HH:mm:ss perchè i tentativi successivi di uno stesso download cadono quasi sempre nello stesso minuto, e con la sola precisione al
     * minuto le loro righe sarebbero indistinguibili.
     *
     * E' una costante statica condivisa da tutti i thread che scrivono nel log, e puo' esserlo perche' DateTimeFormatter e' immutabile.
     */
    private static final DateTimeFormatter FORMATO_ORARIO = DateTimeFormatter.ofPattern("HH:mm:ss");

    private final List<Voce> voci = new ArrayList<>();

    /**
     * Registra una richiesta di download conclusa. 
     * Riceve i campi separati e costruisce lei la Voce, invece di farsi passare una Voce gia' pronta, perche' l'orario va letto sotto il monitor. 
     * Se lo leggesse il chiamante prima di entrare, due NodeHandler potrebbero entrare nel monitor in ordine inverso rispetto agli orari che
     * hanno gia' catturato, e il log stamperebbe una riga delle 13:01:12 sopra una delle 13:01:11. Leggendolo sotto il monitor, l'ordine delle righe e l'ordine degli orari
     * coincidono per costruzione.
     *
     * @param sorgente il peer da cui si scaricava, oppure null quando non esiste (esito NON_DISPONIBILE): nella riga diventa "-".
     */
    public synchronized void aggiungi(String risorsa, String sorgente, String destinazione, Esito esito) {
        String orario = LocalTime.now().format(FORMATO_ORARIO);
        String sorgenteRiga = (sorgente == null) ? "-" : sorgente;
        voci.add(new Voce(orario, risorsa, sorgenteRiga, destinazione, esito));
    }

    /**
     * Le righe del registro nell'ordine in cui sono state scritte.
     *
     * Restituisce una lista NUOVA, non il riferimento a quella interna: chi la scorre lo fa fuori dal monitor, e se scorresse la lista vera 
     * un NodeHandler potrebbe aggiungere una voce durante l'iterazione, facendo fallire il ciclo con una ConcurrentModificationException. 
     * La copia e' costruita dentro il monitor, quindi e' un'istantanea coerente del log in quell'istante.
     *
     * Le righe NON hanno il trattino iniziale e non sono precedute dall'intestazione "Risorse scaricate:": quelli sono presentazione e li aggiunge AggregatorConsole,
     * esattamente come fa per "listdata" con le righe di AggregatorState.
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