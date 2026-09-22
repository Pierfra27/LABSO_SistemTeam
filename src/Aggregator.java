import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Il processo aggregatore: accetta le connessioni dei nodi sensore e avvia un NodeHandler per ciascuna.
 *
 * E' una classe con un'istanza e non un solo main statico perche' due soggetti esterni devono poter chiamare dei metodi su di essa: 
 * la console, che sul comando quit invoca arresta(), e ogni NodeHandler, che al termine si deregistra con rimuoviHandler(). 
 * Con soli campi statici entrambe le cose funzionerebbero comunque, ma lo stato condiviso dell'aggregatore diventerebbe globale 
 * e raggiungibile da chiunque: passare il riferimento rende esplicito chi dipende da cosa.
 *
 * Gli oggetti di stato (AggregatorState, TokenManager, DownloadLog) sono creati QUI, uno ciascuno, e passati a tutti gli handler: 
 * sono loro le risorse condivise del progetto, e l'unicita' dell'istanza e' cio' che rende significativa la mutua esclusione sui rispettivi
 * monitor. Due istanze di AggregatorState darebbero due tabelle diverse e due lock che non si vedono fra loro.
 */

public class Aggregator {

    private final ServerSocket serverSocket;

    // Una sola istanza ciascuno, condivisa da tutti i NodeHandler.
    private final AggregatorState stato = new AggregatorState();
    private final TokenManager token = new TokenManager();
    private final DownloadLog log = new DownloadLog();

    /**
     * La lista dei NodeHandler ancora vivi, e' l'unico stato condiviso che appartiene ad Aggregator, ed e' una risorsa condivisa vera: 
     * ci scrive l'accept loop a ogni connessione accettata, ci scrive ogni handler quando termina e si deregistra, e la
     * legge arresta() sul comando quit per chiudere i socket.
     * Il monitor e' la lista stessa: synchronized (handlers) su ogni accesso. La lista resta privata e si espone solo attraverso aggiungiHandler/rimuoviHandler, 
     * cosi' nessun'altra classe deve sapere su quale oggetto ci si sincronizza;
     */
    private final List<NodeHandler> handlers = new ArrayList<>();

    private Aggregator(ServerSocket serverSocket) {
        this.serverSocket = serverSocket;
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Uso: java -cp bin Aggregator <porta>");
            System.exit(1);
        }

        int porta = 0;
        try {
            porta = Protocol.parseInt(args[0]);
        } catch (IOException e) {
            System.err.println("Errore: la porta deve essere un numero: " + args[0]);
            System.exit(1);
        }
        if (porta < 1 || porta > 65535) {
            System.err.println("Errore: porta fuori dall'intervallo 1-65535: " + porta);
            System.exit(1);
        }

        // Il ServerSocket si apre qui, una volta sola e fuori dal ciclo di accept: se la porta e' gia' occupata il problema e' definitivo 
        // e va segnalato subito con un messaggio comprensibile. Aprirlo dentro il ciclo produrrebbe un errore a ogni giro.
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(porta);
        } catch (IOException e) {
            System.err.println("Errore: impossibile mettersi in ascolto sulla porta " + porta
                    + " (" + e.getMessage() + ")");
            System.exit(1);
        }

        System.out.println("Aggregatore in ascolto sulla porta " + porta + ".");
        new Aggregator(serverSocket).avvia();
    }

    /**
     * La console parte su un thread proprio PRIMA dell'accept loop, e il thread main resta a fare accept. Se fosse il contrario, 
     * cioe' se il main leggesse da tastiera, nessun nodo potrebbe connettersi finche' l'utente non preme invio: la sessione interattiva
     * interromperebbe il funzionamento dell'aggregatore nella rete.
     */
    private void avvia() {
        new Thread(new AggregatorConsole(stato, log, this)).start();
        accettaConnessioni();
    }

    /**
     * Fra una accept e l'altra non c'e' nulla di lento: si crea l'handler, lo si registra e si avvia il suo thread, poi si torna subito ad accettare. 
     * Tutto il dialogo con il nodo avviene sul thread dell'handler.
     */
    private void accettaConnessioni() {
        while (true) {
            Socket socket;
            try {
                socket = serverSocket.accept();
            } catch (IOException e) {
                // Non e' un errore da gestire: e' il modo in cui arresta() fa terminare questo ciclo. Chiudendo il ServerSocket da un altro thread, 
                // l'accept bloccata solleva IOException, che qui indica "smetti di accettare".
                return;
            }
            NodeHandler handler = new NodeHandler(socket, stato, token, log, this);
            aggiungiHandler(handler);
            new Thread(handler).start();
        }
    }

    public void aggiungiHandler(NodeHandler handler) {
        synchronized (handlers) {
            handlers.add(handler);
        }
    }

    public void rimuoviHandler(NodeHandler handler) {
        synchronized (handlers) {
            handlers.remove(handler);
        }
    }

    /**
     * Chiusura ordinata sul comando quit della console. L'ordine dei tre passi non e' arbitrario.
     *
     * 1. Si chiude per primo il ServerSocket: l'accept in corso solleva IOException, il ciclo esce e da quel momento non entrano nuove connessioni.
     *
     * 2. Si copia la lista degli handler dentro il monitor e si chiudono i socket FUORI.
     *
     * 3. System.exit(0): chiudere i socket fa uscire gli handler fermi in lettura, ma non quelli fermi nella wait() di TokenManager.acquisisci, 
     *    che non stanno usando il socket e quindi non si accorgono di nulla.
     */
    public void arresta() {
        try {
            serverSocket.close();
        } catch (IOException e) {
            // Gia' chiuso o mai aperto del tutto: si sta terminando comunque.
        }

        List<NodeHandler> copia;
        synchronized (handlers) {
            copia = new ArrayList<>(handlers);
        }
        for (NodeHandler handler : copia) {
            handler.chiudi();
        }

        System.out.println("Aggregatore arrestato.");
        System.exit(0);
    }
}