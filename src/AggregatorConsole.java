import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

/**
 * La sessione interattiva dell'aggregatore: listdata, log, quit, help.
 *
 * Gira su un thread proprio, avviato da Aggregator prima dell'accept loop, perche' la lettura da tastiera e' bloccante e non deve mai interrompere il funzionamento
 * dell'aggregatore nella rete.
 *
 * Non possiede stato condiviso: legge quello altrui chiamando un oggetto alla volta, e non tiene mai un monitor mentre stampa.
 */
public class AggregatorConsole implements Runnable {

    private final AggregatorState stato;
    private final DownloadLog log;
    private final Aggregator aggregatore;

    public AggregatorConsole(AggregatorState stato, DownloadLog log, Aggregator aggregatore) {
        this.stato = stato;
        this.log = log;
        this.aggregatore = aggregatore;
    }

    @Override
    public void run() {
        BufferedReader tastiera = new BufferedReader(new InputStreamReader(System.in));
        try {
            while (true) {
                System.out.print("> ");
                String riga = tastiera.readLine();
                if (riga == null) {
                    // Termina SOLO questo thread: l'aggregatore continua a servire la rete.
                    System.out.println();
                    return;
                }

                String[] campi = Protocol.split(riga);
                if (campi.length == 0) {
                    continue; // riga vuota: nessun comando, nessun messaggio di errore
                }
                esegui(campi[0]);
            }
        } catch (IOException e) {
            System.err.println("Console terminata: " + e.getMessage());
        }
    }

    private void esegui(String comando) {
        if (comando.equals("listdata")) {
            listdata();
        } else if (comando.equals("log")) {
            log();
        } else if (comando.equals("quit")) {
            aggregatore.arresta();
        } else if (comando.equals("help")) {
            help();
        } else {
            // La console non termina mai per un input sbagliato: chiuderla toglierebbe all'utente l'unico modo di arrestare ordinatamente l'aggregatore.
            System.out.println("Comando non riconosciuto. Digita 'help'.");
        }
    }

    /**
     * Le righe arrivano nel formato: "temp_bo peer0 peer1",  e qui diventano nella forma richiesta: "- temp_bo: peer0, peer1"
     *
     * La conversione sta nella console e non in AggregatorState perche' la tabella custodisce i dati e non decide come mostrarli: 
     * la stessa riga serve tale e quale alla risposta LIST sul socket, e la stessa conversione la fa ClientConsole per "listdata remote".
     *
     * elencoRisorse restituisce gia' una copia, quindi la stampa avviene fuori dal monitor della tabella: nessun nodo resta in attesa mentre l'utente guarda l'elenco.
     */
    private void listdata() {
        List<String> righe = stato.elencoRisorse(null);
        System.out.println("Risorse:");
        for (String riga : righe) {
            String[] campi = Protocol.split(riga);
            StringBuilder linea = new StringBuilder("- ").append(campi[0]).append(':');
            for (int i = 1; i < campi.length; i++) {
                linea.append(i == 1 ? " " : ", ").append(campi[i]);
            }
            System.out.println(linea);
        }
    }

    // Le righe del registro arrivano gia' formattate: qui si aggiunge solo il trattino
    private void log() {
        List<String> righe = log.righe();
        System.out.println("Risorse scaricate:");
        for (String riga : righe) {
            System.out.println("- " + riga);
        }
    }

    private void help() {
        System.out.println("listdata  elenca le rilevazioni della rete e chi le possiede");
        System.out.println("log       elenca tutte le richieste di download con il loro esito");
        System.out.println("quit      chiude le connessioni e arresta l'aggregatore");
        System.out.println("help      mostra questo elenco");
    }
}
