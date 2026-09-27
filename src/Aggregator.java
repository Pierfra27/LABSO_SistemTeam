import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Il processo aggregatore: accetta le connessioni dei nodi e avvia un NodeHandler per ciascuna.
 * Crea una sola istanza di AggregatorState, TokenManager e DownloadLog, condivisa da tutti gli
 * handler: e' l'unicita' dell'istanza che rende efficace la mutua esclusione sui loro monitor.
 */
public class Aggregator {

    private final ServerSocket serverSocket;

    // Una sola istanza ciascuno, condivisa da tutti i NodeHandler.
    private final AggregatorState stato = new AggregatorState();
    private final TokenManager token = new TokenManager();
    private final DownloadLog log = new DownloadLog();

    /**
     * I NodeHandler ancora vivi. E' una risorsa condivisa: ci scrivono l'accept loop e gli
     * handler che terminano, la legge arresta(). Ogni accesso e' in synchronized (handlers).
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

        // Aperto una volta sola, fuori dal ciclo: se la porta e' occupata ci si ferma subito.
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
     * La console gira su un thread proprio e il main resta ad accettare connessioni: cosi' la
     * lettura da tastiera, che e' bloccante, non ferma mai il servizio verso i nodi.
     */
    private void avvia() {
        new Thread(new AggregatorConsole(stato, log, this)).start();
        accettaConnessioni();
    }

    /**
     * Fra una accept e l'altra non si fa nulla di lento: il dialogo con il nodo avviene tutto
     * sul thread del suo handler.
     */
    private void accettaConnessioni() {
        while (true) {
            Socket socket;
            try {
                socket = serverSocket.accept();
            } catch (IOException e) {
                // Non e' un errore: e' arresta() che chiude il ServerSocket per fermare il ciclo.
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
     * Chiusura ordinata sul quit: prima il ServerSocket, poi i socket degli handler (su una
     * copia della lista, fuori dal monitor), infine exit. L'exit serve per gli handler fermi
     * nella wait() del TokenManager, che la chiusura dei socket non sveglia.
     */
    public void arresta() {
        try {
            serverSocket.close();
        } catch (IOException e) {
            // Gia' chiuso: si sta terminando comunque.
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