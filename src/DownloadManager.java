import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

// Gestisce il download di una rilevazione da un altro nodo,
// compresi il contatto con l'aggregatore, la FETCH e gli eventuali retry.
public class DownloadManager {

    private static final int MAX_TENTATIVI = 50;
    private static final int CONNECT_TIMEOUT = 5_000;
    private static final int READ_TIMEOUT = 10_000;

    private final AggregatorLink link;
    private final LocalStore localStore;

    public DownloadManager(AggregatorLink link, LocalStore localStore) {
        this.link = link;
        this.localStore = localStore;
    }

    /**
     * Scarica una rilevazione scegliendo automaticamente il nodo sorgente.
     * Se un tentativo fallisce, chiede all'aggregatore un altro nodo.
     */
    public void download(String nomeRisorsa) {

        if (localStore.contiene(nomeRisorsa)) {
            System.out.println(
                    "Rilevazione '" + nomeRisorsa + "' gia' presente in locale."
            );
            return;
        }

        List<String> esclusi = new ArrayList<>();

        for (int tentativo = 1; tentativo <= MAX_TENTATIVI; tentativo++) {

            String esclusiCsv;

            if (esclusi.isEmpty()) {
                esclusiCsv = Protocol.NONE;
            } else {
                esclusiCsv = String.join(",", esclusi);
            }

            String richiesta =
                    Protocol.REQ_RESOLVE + " "
                            + nomeRisorsa + " "
                            + Protocol.NONE + " "
                            + esclusiCsv;

            /*
             * La RESOLVE puo' restare in attesa mentre il token
             * del nodo sorgente e' occupato.
             */
            AggregatorLink.Risposta risposta =
                    link.inviaResolve(richiesta);

            if (!risposta.ok) {

                if (notFound(risposta)) {
                    System.out.println(
                            "Rilevazione '" + nomeRisorsa
                                    + "' non disponibile sulla rete."
                    );
                } else {
                    stampaErroreAggregatore(risposta);
                }

                return;
            }

            /*
             * Risposta attesa:
             * OK <token> <peerId> <host> <porta>
             */
            if (risposta.campi.length < 5) {
                System.err.println(
                        "Errore: risposta RESOLVE non valida."
                );
                return;
            }

            String token = risposta.campi[1];
            String peer = risposta.campi[2];
            String host = risposta.campi[3];

            int porta;

            try {
                porta = Protocol.parseInt(risposta.campi[4]);
            } catch (IOException e) {

                // Il token e' stato acquisito: va comunque rilasciato.
                rilascia(token, Protocol.OUTCOME_FAIL,
                        nomeRisorsa, Protocol.NONE);

                System.err.println(
                        "Errore: porta del peer non valida."
                );

                return;
            }

            EsitoFetch esito =
                    provaFetch(host, porta, nomeRisorsa, token);

            if (esito.riuscito) {

                boolean salvata =
                        localStore.aggiungi(nomeRisorsa, esito.contenuto);

                if (!salvata) {

                    /*
                     * La FETCH e' riuscita, quindi non dobbiamo accusare
                     * il peer sorgente di non possedere la risorsa.
                     * Usiamo "-" per liberare semplicemente il token.
                     */
                    rilascia(
                            token,
                            Protocol.OUTCOME_FAIL,
                            nomeRisorsa,
                            Protocol.NONE
                    );

                    System.err.println(
                            "Errore: impossibile salvare la rilevazione in locale."
                    );

                    return;
                }

                AggregatorLink.Risposta release =
                        rilascia(
                                token,
                                Protocol.OUTCOME_OK,
                                nomeRisorsa,
                                peer
                        );

                if (!release.ok) {
                    stampaErroreAggregatore(release);
                    return;
                }

                System.out.println(
                        "Scaricata '" + nomeRisorsa
                                + "' da " + peer
                                + " (" + esito.contenuto.length + " byte)."
                );

                return;
            }

            /*
             * Il tentativo sul peer e' fallito.
             * PRIMA liberiamo il token del peer,
             * POI chiediamo un nuovo candidato.
             */
            System.out.println(
                    "Tentativo su " + peer
                            + " fallito (" + esito.motivo + "), riprovo..."
            );

            AggregatorLink.Risposta release =
                    rilascia(
                            token,
                            Protocol.OUTCOME_FAIL,
                            nomeRisorsa,
                            peer
                    );

            /*
             * Se e' caduto il collegamento con l'aggregatore,
             * non possiamo proseguire con il retry.
             * Un BADTOKEN invece puo' accadere, ad esempio, se il
             * token e' gia' stato liberato dopo la caduta del sorgente:
             * in quel caso possiamo comunque provare un altro peer.
             */
            if (!release.ok && link.linkCaduto()) {
                stampaErroreAggregatore(release);
                return;
            }

            if (!esclusi.contains(peer)) {
                esclusi.add(peer);
            }
        }

        System.out.println(
                "Troppi tentativi, download interrotto."
        );
    }

    /**
     * Download mirato: l'utente sceglie direttamente il peer.
     * Non viene eseguito alcun retry su altri nodi.
     */
    public void download(String peerDestinazione, String nomeRisorsa) {

        if (localStore.contiene(nomeRisorsa)) {
            System.out.println(
                    "Rilevazione '" + nomeRisorsa + "' gia' presente in locale."
            );
            return;
        }

        String richiesta =
                Protocol.REQ_RESOLVE_AT + " "
                        + nomeRisorsa + " "
                        + peerDestinazione + " "
                        + Protocol.NONE;

        AggregatorLink.Risposta risposta =
                link.inviaResolve(richiesta);

        if (!risposta.ok) {

            if (notFound(risposta)) {
                System.out.println(
                        "Rilevazione '" + nomeRisorsa
                                + "' non disponibile su " + peerDestinazione + "."
                );
            } else {
                stampaErroreAggregatore(risposta);
            }

            return;
        }

        if (risposta.campi.length < 5) {
            System.err.println(
                    "Errore: risposta RESOLVE_AT non valida."
            );
            return;
        }

        String token = risposta.campi[1];
        String peer = risposta.campi[2];
        String host = risposta.campi[3];

        int porta;

        try {
            porta = Protocol.parseInt(risposta.campi[4]);
        } catch (IOException e) {

            rilascia(
                    token,
                    Protocol.OUTCOME_FAIL,
                    nomeRisorsa,
                    Protocol.NONE
            );

            System.err.println(
                    "Errore: porta del peer non valida."
            );

            return;
        }

        EsitoFetch esito =
                provaFetch(host, porta, nomeRisorsa, token);

        if (!esito.riuscito) {

            rilascia(
                    token,
                    Protocol.OUTCOME_FAIL,
                    nomeRisorsa,
                    peer
            );

            System.out.println(
                    "Download da " + peer
                            + " fallito: " + esito.motivo
            );

            return;
        }

        boolean salvata =
                localStore.aggiungi(nomeRisorsa, esito.contenuto);

        if (!salvata) {

            rilascia(
                    token,
                    Protocol.OUTCOME_FAIL,
                    nomeRisorsa,
                    Protocol.NONE
            );

            System.err.println(
                    "Errore: impossibile salvare la rilevazione in locale."
            );

            return;
        }

        AggregatorLink.Risposta release =
                rilascia(
                        token,
                        Protocol.OUTCOME_OK,
                        nomeRisorsa,
                        peer
                );

        if (!release.ok) {
            stampaErroreAggregatore(release);
            return;
        }

        System.out.println(
                "Scaricata '" + nomeRisorsa
                        + "' da " + peer
                        + " (" + esito.contenuto.length + " byte)."
        );
    }

    /**
     * Esegue materialmente la FETCH verso un altro nodo.
     */
    private EsitoFetch provaFetch(
            String host,
            int porta,
            String nomeRisorsa,
            String token) {

        try (Socket socket = new Socket()) {

            socket.connect(
                    new InetSocketAddress(host, porta),
                    CONNECT_TIMEOUT
            );

            socket.setSoTimeout(READ_TIMEOUT);

            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            Protocol.writeLine(
                    out,
                    Protocol.REQ_FETCH + " "
                            + nomeRisorsa + " "
                            + token
            );

            String primaRiga = Protocol.readLine(in);

            if (primaRiga == null) {
                return EsitoFetch.fallito(
                        "connessione chiusa dal peer"
                );
            }

            String[] risposta = Protocol.split(primaRiga);

            if (risposta.length == 0 ||
                    !risposta[0].equals(Protocol.RESP_OK)) {

                return EsitoFetch.fallito(
                        "risorsa non disponibile"
                );
            }

            String rigaLength = Protocol.readLine(in);

            if (rigaLength == null) {
                return EsitoFetch.fallito(
                        "manca la lunghezza del contenuto"
                );
            }

            String[] campiLength =
                    Protocol.split(rigaLength);

            if (campiLength.length != 2 ||
                    !campiLength[0].equals(Protocol.RESP_LENGTH)) {

                return EsitoFetch.fallito(
                        "risposta LENGTH non valida"
                );
            }

            int lunghezza =
                    Protocol.parseInt(campiLength[1]);

            byte[] contenuto =
                    Protocol.readFully(in, lunghezza);

            return EsitoFetch.riuscito(contenuto);

        } catch (IOException e) {

            return EsitoFetch.fallito(
                    e.getMessage() == null
                            ? "errore di comunicazione"
                            : e.getMessage()
            );
        }
    }

    /**
     * Comunica all'aggregatore la fine di un tentativo
     * e libera il token del nodo sorgente.
     */
    private AggregatorLink.Risposta rilascia(
            String token,
            String esito,
            String nomeRisorsa,
            String peerSorgente) {

        String richiesta =
                Protocol.REQ_RELEASE + " "
                        + token + " "
                        + esito + " "
                        + nomeRisorsa + " "
                        + peerSorgente;

        return link.invia(richiesta);
    }

    private boolean notFound(AggregatorLink.Risposta risposta) {

        return risposta.campi.length >= 2
                && risposta.campi[0].equals(Protocol.RESP_ERR)
                && risposta.campi[1].equals(Protocol.ERR_NOTFOUND);
    }

    private void stampaErroreAggregatore(
            AggregatorLink.Risposta risposta) {

        if (link.linkCaduto()) {
            System.err.println(
                    "Errore: aggregatore non piu' raggiungibile."
            );
            return;
        }

        if (risposta.campi.length >= 2) {
            System.err.println(
                    "Errore dell'aggregatore: "
                            + risposta.campi[1]
            );
        } else {
            System.err.println(
                    "Errore: risposta non valida dall'aggregatore."
            );
        }
    }

    /**
     * Piccolo oggetto che rappresenta il risultato
     * di un tentativo di FETCH.
     */
    private static class EsitoFetch {

        final boolean riuscito;
        final byte[] contenuto;
        final String motivo;

        private EsitoFetch(
                boolean riuscito,
                byte[] contenuto,
                String motivo) {

            this.riuscito = riuscito;
            this.contenuto = contenuto;
            this.motivo = motivo;
        }

        static EsitoFetch riuscito(byte[] contenuto) {
            return new EsitoFetch(
                    true,
                    contenuto,
                    null
            );
        }

        static EsitoFetch fallito(String motivo) {
            return new EsitoFetch(
                    false,
                    null,
                    motivo
            );
        }
    }
}
