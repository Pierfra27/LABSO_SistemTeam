import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
 
/** Questa classe si occupa del server del nodo sensore: resta in ascolto delle connessioni degli altri peer
 e crea un PeerHandler separato per gestire ogni richiesta ricevuta.
 */
public class PeerServer implements Runnable {
 
    private final ServerSocket serverSocket;
    private final LocalStore localStore;
    private final PeerAccessLock accessLock;
 
    public PeerServer(LocalStore localStore) throws IOException {
        this.localStore = localStore;
        this.accessLock = new PeerAccessLock();
 
        // Con porta 0 sara' il sistema operativo a scegliere
        // automaticamente una porta libera.
        this.serverSocket = new ServerSocket(0);
    }
 
    /**
     Restituisce la porta scelta dal sistema operativo.
     Servira' al Client per comunicarla all'aggregatore durante la REGISTER.
     */
    public int getPorta() {
        return serverSocket.getLocalPort();
    }
 
     //Rimane in ascolto delle connessioni provenienti dagli altri nodi sensore.
    @Override
    public void run() {
 
        try {
 
            while (!serverSocket.isClosed()) {
 
                Socket peerSocket = serverSocket.accept();
 
                PeerHandler handler =
                        new PeerHandler(peerSocket, localStore, accessLock);
 
                new Thread(handler).start();
            }
 
        } catch (IOException e) {
 
            // Se il ServerSocket e' stato chiuso volontariamente,
            // l'eccezione di accept() e' normale.
            if (!serverSocket.isClosed()) {
                System.err.println(
                        "Errore nel PeerServer: " + e.getMessage()
                );
            }
        }
    }
 
    //Arresta il PeerServer La close() del ServerSocket sblocca anche un eventuale thread fermo dentro accept().
    public void chiudi() {
 
        try {
            serverSocket.close();
        } catch (IOException e) {
            System.err.println(
                    "Errore durante la chiusura del PeerServer: "
                            + e.getMessage()
            );
        }
    }
}