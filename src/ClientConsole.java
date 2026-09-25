import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

/**
 * La sessione interattiva del nodo sensore: legge i comandi da tastiera, li esegue chiamando
 * LocalStore, AggregatorLink o DownloadManager, e stampa il risultato nei formati richiesti da A.5.
 *
 * Gira sul thread main del nodo. Mentre e' ferma (in attesa di un comando, o dentro un
 * download che blocca a lungo) il PeerServer continua a rispondere agli altri nodi su un
 * thread proprio: il nodo resta raggiungibile anche a console bloccata.
 *
 * Non possiede stato condiviso proprio: usa quello altrui chiamando un oggetto alla volta.
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
                    System.out.println();
                    quit();
                    return;
                }

                // maxParts = 3: "add <nome> <contenuto>" -> il contenuto prende tutto il resto
                // della riga, spazi interni compresi.
                String[] campi = Protocol.split(riga, 3);
                if (campi.length == 0) {
                    continue;
                }
                esegui(campi);
            }
        } catch (IOException e) {
            System.err.println("Console terminata: " + e.getMessage());
        }
    }

    private void esegui(String[] campi) throws IOException {
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
            System.out.println("Comando non riconosciuto. Digita 'help'.");
        }
    }

    // listdata local | listdata remote [<nome>] | listdata peers
    private void listdata(String[] campi) throws IOException {
        if (campi.length < 2) {
            System.out.println("Uso: listdata local|remote|peers");
            return;
        }
        String sotto = campi[1];

        if (sotto.equals("local")) {
            listdataLocal();
        } else if (sotto.equals("remote")) {
            String nome = null;
            if (campi.length >= 3) {
                nome = campi[2];
            }
            listdataRemote(nome);
        } else if (sotto.equals("peers")) {
            listdataPeers();
        } else {
            System.out.println("Uso: listdata local|remote|peers");
        }
    }

    // Nessuna rete coinvolta: solo LocalStore.
    private void listdataLocal() {
        List<String> nomi = store.elenco();
        System.out.println("Risorse:");
        for (String nome : nomi) {
            System.out.println("- " + nome);
        }
    }

    // LIST <nome|-> all'aggregatore, poi si formatta "risorsa: peer, peer, ..."
    private void listdataRemote(String nome) throws IOException {
        String argomento = Protocol.NONE;
        if (nome != null) {
            argomento = nome;
        }

        AggregatorLink.Risposta risposta = link.invia(Protocol.REQ_LIST + " " + argomento);

        if (!risposta.ok) {
            stampaErrore(risposta);
            return;
        }

        System.out.println("Risorse:");
        for (String riga : risposta.righeExtra) {
            String[] pezzi = Protocol.split(riga);

            String linea = "- " + pezzi[0] + ":";
            for (int i = 1; i < pezzi.length; i++) {
                if (i == 1) {
                    linea = linea + " " + pezzi[i];
                } else {
                    linea = linea + ", " + pezzi[i];
                }
            }
            System.out.println(linea);
        }
    }

    // PEERS all'aggregatore: solo i nodi attivi, esclude se stesso.
    private void listdataPeers() throws IOException {
        AggregatorLink.Risposta risposta = link.invia(Protocol.REQ_PEERS);

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

    // add <nome> <contenuto...>
    private void add(String[] campi) throws IOException {
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

        boolean salvata = store.aggiungi(nome, contenuto.getBytes());
        if (!salvata) {
            System.out.println("Errore: impossibile salvare '" + nome + "'.");
            return;
        }

        AggregatorLink.Risposta risposta = link.invia(Protocol.REQ_ADDED + " " + nome);
        if (!risposta.ok) {
            stampaErrore(risposta);
            return;
        }
        System.out.println("Rilevazione '" + nome + "' aggiunta e notificata all'aggregatore.");
    }

    // remove <nome>
    private void remove(String[] campi) throws IOException {
        if (campi.length < 2) {
            System.out.println("Uso: remove <nome>");
            return;
        }
        String nome = campi[1];

        if (!store.contiene(nome)) {
            System.out.println("Errore: rilevazione '" + nome + "' non presente.");
            return;
        }

        boolean rimossa = store.rimuovi(nome);
        if (!rimossa) {
            System.out.println("Errore: impossibile rimuovere '" + nome + "'.");
            return;
        }

        AggregatorLink.Risposta risposta = link.invia(Protocol.REQ_REMOVED + " " + nome);
        if (!risposta.ok) {
            stampaErrore(risposta);
            return;
        }
        System.out.println("Rilevazione '" + nome + "' rimossa e notificata all'aggregatore.");
    }

    // download <nome>  oppure  download <peer> <nome>
    private void download(String[] campi) {
        if (campi.length == 2) {
            downloadManager.scarica(campi[1]);
        } else if (campi.length == 3) {
            downloadManager.scaricaDa(campi[1], campi[2]);
        } else {
            System.out.println("Uso: download <nome>  oppure  download <peer> <nome>");
        }
    }

    private void help() {
        System.out.println("listdata local          elenca le rilevazioni possedute localmente");
        System.out.println("listdata remote [<nome>] elenca chi possiede una o tutte le rilevazioni");
        System.out.println("listdata peers           elenca gli altri nodi attivi");
        System.out.println("add <nome> <contenuto>   aggiunge una rilevazione locale");
        System.out.println("remove <nome>            rimuove una rilevazione locale");
        System.out.println("download <nome>          scarica una rilevazione dalla rete");
        System.out.println("download <peer> <nome>   scarica una rilevazione da un nodo specifico");
        System.out.println("help                     mostra questo elenco");
        System.out.println("quit                     chiude il nodo");
    }

    private void quit() {
        link.invia(Protocol.REQ_DISCONNECT);
        System.out.println("Nodo arrestato.");
        System.exit(0);
    }

    // Un unico punto per stampare gli errori di rete in modo uniforme.
    private void stampaErrore(AggregatorLink.Risposta risposta) {
        if (risposta.campi.length >= 2 && risposta.campi[1].equals("LINKCADUTO")) {
            System.out.println("Errore: aggregatore non piu' raggiungibile.");
            return;
        }

        String messaggio = "";
        for (int i = 0; i < risposta.campi.length; i++) {
            if (i > 0) {
                messaggio = messaggio + " ";
            }
            messaggio = messaggio + risposta.campi[i];
        }
        System.out.println("Errore: " + messaggio);
    }
}