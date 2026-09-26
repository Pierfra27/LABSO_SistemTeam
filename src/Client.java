import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.List;

public class Client {

    public static void main(String[] args) {

        // --- 1. Parsing argomenti ---
        if (args.length < 2 || args.length > 3) {
            System.err.println("Uso: java -cp bin Client <host> <porta> [<cartellaDati>]");
            System.exit(1);
        }

        String host = args[0];
        int porta = 0;
        try {
            porta = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.err.println("Errore: la porta deve essere un numero: " + args[1]);
            System.exit(1);
        }
        if (porta < 1 || porta > 65535) {
            System.err.println("Errore: porta fuori dall'intervallo 1-65535: " + porta);
            System.exit(1);
        }

        String cartellaDati = "data/node";
        if (args.length == 3) {
            cartellaDati = args[2];
        }

        // --- 2. Controllo cartella dati ---
        File cartella = new File(cartellaDati);
        if (!cartella.exists() || !cartella.isDirectory() || !cartella.canRead()) {
            System.err.println("Errore: cartella dati non accessibile: " + cartellaDati);
            System.exit(1);
        }

        LocalStore store = new LocalStore(cartellaDati);

        // --- 3. Connessione all'aggregatore ---
        Socket socket = null;
        AggregatorLink link = null;
        try {
            socket = new Socket(host, porta);
            link = new AggregatorLink(socket);
        } catch (IOException e) {
            System.err.println("Errore: aggregatore non raggiungibile su " + host + ":" + porta);
            System.exit(1);
        }

        // --- 4. Avvio del PeerServer ---
        PeerServer peerServer = null;
        try {
            peerServer = new PeerServer(store);
            peerServer.start();
        } catch (IOException e) {
            System.err.println("Errore: impossibile avviare il servizio verso gli altri nodi.");
            System.exit(1);
        }

        int portaAscolto = peerServer.getPorta();

        // --- 5. REGISTER ---
        // ANCORA BLOCCATO: manca un metodo in AggregatorLink per mandare righe extra
        // in uscita. Da concordare col gruppo (vedi discussione sopra).
        List<String> nomi = store.elenco();
        String richiesta = "REGISTER " + portaAscolto + " " + nomi.size();

        AggregatorLink.Risposta risposta = null; // = link.???(richiesta, nomi);

        if (risposta == null || !risposta.ok) {
            System.err.println("Errore: registrazione rifiutata dall'aggregatore.");
            peerServer.chiudi();
            System.exit(1);
        }

        String peerId = risposta.campi[1];
        System.out.println("Registrato come " + peerId + ".");

        // --- 6. Avvio console ---
        DownloadManager downloadManager = new DownloadManager(link, store);
        ClientConsole console = new ClientConsole(store, link, downloadManager, peerId);
        console.run();
    }
}