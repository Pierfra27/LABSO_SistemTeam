import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * La console del nodo sensore: legge i comandi e stampa i risultati nei formati delle specifiche.
 * Gira sul thread principale; intanto il PeerServer continua a servire gli altri nodi, quindi
 * il nodo resta attivo nella rete anche quando la console e' occupata.
 * BufferedReader si usa solo qui e nell'altra console, perche' legge da tastiera e non da socket.
 */
public class ClientConsole implements Runnable {

    private final LocalStore store;
    private final AggregatorLink link;
    private final DownloadManager downloadManager;
    private final String mioPeerId;

    public ClientConsole(LocalStore store, AggregatorLink link,
                         DownloadManager downloadManager, String mioPeerId) {
        this.store = store;
        this.link = link;
        this.downloadManager = downloadManager;
        this.mioPeerId = mioPeerId;
    }

    @Override
    public void run() {
        BufferedReader tastiera = new BufferedReader(new InputStreamReader(System.in));
        try {
            while (true) {
                System.out.print("> ");
                String riga = tastiera.readLine();

                if (riga == null) {
                    // Fine della tastiera (Ctrl-D): termina solo la console, il nodo resta vivo
                    // e continua a servire gli altri. Ci si ferma solo con quit.
                    System.out.println();
                    return;
                }

                // Al massimo 3 campi, cosi' il contenuto di "add <nome> <contenuto>" resta intero.
                String[] campi = Protocol.split(riga, 3);
                if (campi.length == 0) {
                    continue; // riga vuota
                }
                esegui(campi);
            }
        } catch (IOException e) {
            System.err.println("Console terminata: " + e.getMessage());
        }
    }

    private void esegui(String[] campi) {
        String comando = campi[0];

        if (comando.equals("listdata")) {
            listdata(campi);
        } else if (comando.equals("add")) {
            add(campi);
        } else if (comando.equals("remove")) {
            remove(campi);
        } else if (comando.equals("download")) {
            download(campi);
        } else if (comando.equals("help")) {
            help();
        } else if (comando.equals("quit")) {
            quit();
        } else {
            // Un comando sbagliato non chiude la console.
            System.out.println("Comando non riconosciuto. Digita 'help'.");
        }
    }

    private void listdata(String[] campi) {
        if (campi.length < 2) {
            System.out.println("Uso: listdata local|remote [<nome>]|peers");
            return;
        }
        String sotto = campi[1];

        if (sotto.equals("local")) {
            listdataLocal();
        } else if (sotto.equals("remote")) {
            listdataRemote(campi.length >= 3 ? campi[2] : null);
        } else if (sotto.equals("peers")) {
            listdataPeers();
        } else {
            System.out.println("Uso: listdata local|remote [<nome>]|peers");
        }
    }

    /** Nessuna rete coinvolta: la risposta e' tutta nell'archivio locale. */
    private void listdataLocal() {
        List<String> nomi = store.elenco();
        System.out.println("Risorse:");
        for (String nome : nomi) {
            System.out.println("- " + nome);
        }
    }

    /**
     * Le righe arrivano nel formato del protocollo ("temp_bo peer0 peer1") e qui diventano
     * quello delle specifiche ("- temp_bo: peer0, peer1"). La tabella custodisce i dati, la
     * console decide come mostrarli.
     */
    private void listdataRemote(String nome) {
        String argomento = (nome == null) ? Protocol.NONE : nome;

        AggregatorLink.Risposta risposta = link.invia(Protocol.REQ_LIST + " " + argomento, true);
        if (!risposta.ok) {
            stampaErrore(risposta);
            return;
        }

        System.out.println("Risorse:");
        for (String riga : risposta.righeExtra) {
            String[] pezzi = Protocol.split(riga);
            if (pezzi.length == 0) {
                continue;
            }
            StringBuilder linea = new StringBuilder("- ").append(pezzi[0]).append(':');
            for (int i = 1; i < pezzi.length; i++) {
                linea.append(i == 1 ? " " : ", ").append(pezzi[i]);
            }
            System.out.println(linea);
        }
    }

    /** Il nodo esclude se stesso: l'utente chiede gli ALTRI nodi attivi. */
    private void listdataPeers() {
        AggregatorLink.Risposta risposta = link.invia(Protocol.REQ_PEERS, true);
        if (!risposta.ok) {
            stampaErrore(risposta);
            return;
        }

        System.out.println("Nodi attivi:");
        for (String peerId : risposta.righeExtra) {
            if (!peerId.equals(mioPeerId)) {
                System.out.println("- " + peerId);
            }
        }
    }

    /**
     * Prima si salva su disco, poi si avvisa l'aggregatore: altrimenti, se la scrittura
     * fallisse, la rete crederebbe che il nodo abbia una rilevazione che non ha.
     * Il contenuto si converte con UTF-8, la stessa codifica usata sui socket.
     */
    private void add(String[] campi) {
        if (campi.length < 3) {
            System.out.println("Uso: add <nome> <contenuto>");
            return;
        }
        String nome = campi[1];
        String contenuto = campi[2];

        if (store.contiene(nome)) {
            System.out.println("Errore: rilevazione '" + nome + "' gia' presente.");
            return;
        }
        if (!store.aggiungi(nome, contenuto.getBytes(StandardCharsets.UTF_8))) {
            System.out.println("Errore: impossibile salvare '" + nome + "'.");
            return;
        }

        AggregatorLink.Risposta risposta = link.invia(Protocol.REQ_ADDED + " " + nome, false);
        if (!risposta.ok) {
            stampaErrore(risposta);
            return;
        }
        System.out.println("Rilevazione '" + nome + "' aggiunta e notificata all'aggregatore.");
    }

    /** Stesso ordine dell'add e per lo stesso motivo: prima il disco, poi la notifica. */
    private void remove(String[] campi) {
        if (campi.length < 2) {
            System.out.println("Uso: remove <nome>");
            return;
        }
        String nome = campi[1];

        if (!store.contiene(nome)) {
            System.out.println("Errore: rilevazione '" + nome + "' non presente.");
            return;
        }
        if (!store.rimuovi(nome)) {
            System.out.println("Errore: impossibile rimuovere '" + nome + "'.");
            return;
        }

        AggregatorLink.Risposta risposta = link.invia(Protocol.REQ_REMOVED + " " + nome, false);
        if (!risposta.ok) {
            stampaErrore(risposta);
            return;
        }
        System.out.println("Rilevazione '" + nome + "' rimossa e notificata all'aggregatore.");
    }

    /** download <nome> dalla rete, oppure download <peer> <nome> da un nodo scelto. */
    private void download(String[] campi) {
        if (campi.length == 2) {
            downloadManager.download(campi[1]);
        } else if (campi.length == 3) {
            downloadManager.download(campi[1], campi[2]);
        } else {
            System.out.println("Uso: download <nome>  oppure  download <peer> <nome>");
        }
    }

    private void help() {
        System.out.println("listdata local           elenca le rilevazioni possedute localmente");
        System.out.println("listdata remote [<nome>] elenca chi possiede una o tutte le rilevazioni");
        System.out.println("listdata peers           elenca gli altri nodi attivi");
        System.out.println("add <nome> <contenuto>   aggiunge una rilevazione locale");
        System.out.println("remove <nome>            rimuove una rilevazione locale");
        System.out.println("download <nome>          scarica una rilevazione dalla rete");
        System.out.println("download <peer> <nome>   scarica una rilevazione da un nodo specifico");
        System.out.println("help                     mostra questo elenco");
        System.out.println("quit                     chiude il nodo");
    }

    /**
     * Il DISCONNECT parte prima di terminare: cosi' l'aggregatore sa che il nodo esce di sua
     * volonta' e non e' caduto. Le sue rilevazioni restano in tabella ma non saranno accessibili.
     */
    private void quit() {
        link.invia(Protocol.REQ_DISCONNECT, false);
        System.out.println("Nodo arrestato.");
        System.exit(0);
    }

    /** Un solo punto per gli errori, cosi' il messaggio ha sempre la stessa forma. */
    private void stampaErrore(AggregatorLink.Risposta risposta) {
        if (risposta.campi.length >= 2 && risposta.campi[1].equals(AggregatorLink.LINK_CADUTO)) {
            System.out.println("Errore: aggregatore non piu' raggiungibile.");
            return;
        }
        // Di "ERR NOTFOUND" all'utente serve solo il codice.
        String codice = risposta.campi.length >= 2 ? risposta.campi[1] : "sconosciuto";
        System.out.println("Errore: " + codice);
    }
}