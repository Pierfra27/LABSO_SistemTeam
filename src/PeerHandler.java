import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

// Gestisce una singola richiesta FETCH proveniente da un altro nodo sensore,
// garantendo l'accesso esclusivo al nodo durante il trasferimento.
public class PeerHandler extends Thread {

    private final Socket socket;
    private final LocalStore localStore;
    private final PeerAccessLock accessLock;

    public PeerHandler(Socket socket,
                       LocalStore localStore,
                       PeerAccessLock accessLock) {

        this.socket = socket;
        this.localStore = localStore;
        this.accessLock = accessLock;
    }

    @Override
    public void run() {

        try (Socket s = socket) {

            InputStream in = s.getInputStream();
            OutputStream out = s.getOutputStream();

            // Legge la richiesta inviata dall'altro nodo.
            String richiesta = Protocol.readLine(in);

            if (richiesta == null) {
                return;
            }

            String[] campi = Protocol.split(richiesta);

            // La richiesta deve essere:
            // FETCH <risorsa> <token>
            if (campi.length != 3 ||
                    !campi[0].equals(Protocol.REQ_FETCH)) {

                Protocol.writeLine(
                        out,
                        Protocol.RESP_ERR + " " + Protocol.ERR_BADREQUEST
                );

                return;
            }

            String nomeRisorsa = campi[1];
            String token = campi[2];

            // Non conosciamo il peerId del nodo richiedente tramite FETCH,
            // quindi usiamo il suo indirizzo come informazione diagnostica.
            String richiedente =
                    s.getRemoteSocketAddress().toString();

            if (accessLock.isOccupato()) {
                System.out.println(
                        "[" + richiedente + "] in attesa..."
                );
            }

            /*
             * IMPORTANTE:
             * acquisisci() resta fuori dal try/finally del rilascio.
             * Se venisse interrotto mentre e' in wait(), il lock
             * non sarebbe stato acquisito e quindi non dovremmo rilasciarlo.
             */
            try {
                accessLock.acquisisci(richiedente);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            try {

                System.out.println(
                        "[" + richiedente + "] servo FETCH "
                                + nomeRisorsa + " (token " + token + ")"
                );

                // Controlla che la rilevazione esista.
                if (!localStore.contiene(nomeRisorsa)) {

                    Protocol.writeLine(
                            out,
                            Protocol.RESP_ERR + " "
                                    + Protocol.ERR_NOTFOUND
                    );

                    return;
                }

                byte[] contenuto;

                try {
                    contenuto = localStore.leggi(nomeRisorsa);
                } catch (IOException e) {

                    Protocol.writeLine(
                            out,
                            Protocol.RESP_ERR + " "
                                    + Protocol.ERR_NOTFOUND
                    );

                    return;
                }

                /*
                 * Risposta: in n byte
                 */
                Protocol.writeLine(
                        out,
                        Protocol.RESP_OK
                );

                Protocol.writePayload(
                        out,
                        contenuto
                );

                System.out.println(
                        "[" + richiedente + "] FETCH completata: "
                                + nomeRisorsa
                );

            } finally {

                // Il nodo deve tornare disponibile anche se durante
                // il trasferimento avviene un errore.
                accessLock.rilascia();
            }

        } catch (IOException e) {

            System.err.println(
                    "Errore nel PeerHandler: " + e.getMessage()
            );
        }
    }
}
