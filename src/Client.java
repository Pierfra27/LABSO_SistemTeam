import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.List;
 
/**
 * Il processo nodo sensore. Si avvia con l'indirizzo e la porta dell'aggregatore, e accetta un terzo argomento opzionale, la cartella delle rilevazioni, 
 * che serve a far girare piu' nodi con contenuti diversi sulla stessa macchina durante la demo.
 */
public class Client {
 
    //Cartella usata quando il terzo argomento manca.
    private static final String CARTELLA_DEFAULT = "data/default";
 
    public static void main(String[] args) {
 
        // --- 1. Argomenti ---
        if (args.length < 2 || args.length > 3) {
            System.err.println("Uso: java -cp bin Client <host> <porta> [<cartellaDati>]");
            System.exit(1);
        }
 
        String host = args[0];
        int porta = 0;
        try {
            porta = Protocol.parseInt(args[1]);
        } catch (IOException e) {
            System.err.println("Errore: la porta deve essere un numero: " + args[1]);
            System.exit(1);
        }
        if (porta < 1 || porta > 65535) {
            System.err.println("Errore: porta fuori dall'intervallo 1-65535: " + porta);
            System.exit(1);
        }
 
        // --- 2. Archivio locale ---
        // Senza terzo argomento si usa la cartella di default, e se non esiste la si crea vuota.
        String cartellaDati = CARTELLA_DEFAULT;
        if (args.length == 3) {
            cartellaDati = args[2];
        } else {
            new File(cartellaDati).mkdirs();
        }
 
        LocalStore store = null;
        try {
            store = new LocalStore(cartellaDati);
        } catch (IOException e) {
            System.err.println("Errore: " + e.getMessage());
            System.exit(1);
        }
 
        // --- 3. Connessione all'aggregatore ---
        AggregatorLink link = null;
        try {
            Socket socket = new Socket(host, porta);
            link = new AggregatorLink(socket);
        } catch (IOException e) {
            System.err.println("Errore: aggregatore non raggiungibile su " + host + ":" + porta);
            System.exit(1);
        }
 
        // --- 4. PeerServer, prima della REGISTER che deve dichiararne la porta ---
        PeerServer peerServer = null;
        try {
            peerServer = new PeerServer(store);
        } catch (IOException e) {
            System.err.println("Errore: impossibile avviare il servizio verso gli altri nodi.");
            System.exit(1);
        }
        new Thread(peerServer).start();
 
        // --- 5. REGISTER: intestazione piu' una riga per rilevazione, in un unico scambio ---
        List<String> nomi = store.elenco();
        String richiesta = Protocol.REQ_REGISTER + " " + peerServer.getPorta() + " " + nomi.size();
        AggregatorLink.Risposta risposta = link.invia(richiesta, nomi);
 
        if (!risposta.ok || risposta.campi.length < 2) {
            System.err.println("Errore: registrazione rifiutata dall'aggregatore.");
            peerServer.chiudi();
            System.exit(1);
        }
 
        String peerId = risposta.campi[1];
        System.out.println("Registrato come " + peerId + ", in ascolto sulla porta "
                + peerServer.getPorta() + ".");
 
        // --- 6. Console, sul thread principale ---
        DownloadManager downloadManager = new DownloadManager(link, store);
        new ClientConsole(store, link, downloadManager, peerId).run();
    }
}