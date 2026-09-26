import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
 
/**
 * L'archivio locale delle rilevazioni di questo nodo: una cartella di file <nome>.txt piu' un indice dei nomi tenuto in memoria e ordinato.
 *
 * E' UNA RISORSA CONDIVISA, ed e' la ragione per cui ogni metodo pubblico e' synchronized:
 * ci accede il thread della console quando l'utente digita add, remove o listdata local, e ci accede in parallelo il PeerHandler quando un altro 
 * nodo della rete scarica una rilevazione da qui. 
 */
public class LocalStore {
 
    private final File cartella;
 
    //I nomi delle rilevazioni possedute, senza estensione, tenuti ordinati.
    private final List<String> nomi = new ArrayList<>();
 
    //Estensione dei file dell'archivio: il nome della rilevazione e' il resto.
    private static final String ESTENSIONE = ".txt";
 
    /**
     * Apre l'archivio e costruisce l'indice leggendo la cartella.
     */
    public LocalStore(String percorsoCartella) throws IOException {
        this.cartella = new File(percorsoCartella);
 
        if (!cartella.isDirectory()) {
            throw new IOException("cartella dati non accessibile: " + percorsoCartella);
        }
        File[] contenuto = cartella.listFiles();
        if (contenuto == null) {
            throw new IOException("cartella dati non leggibile: " + percorsoCartella);
        }
 
        for (File f : contenuto) {
            // Si prendono SOLO i file che finiscono per .txt. Senza questo controllo qualunque altro file della cartella 
            // diventerebbe una rilevazione con un nome storpiato.
            String nomeFile = f.getName();
            if (f.isFile() && nomeFile.endsWith(ESTENSIONE)) {
                nomi.add(nomeFile.substring(0, nomeFile.length() - ESTENSIONE.length()));
            }
        }
        Collections.sort(nomi);
    }
 
    /**
     * I nomi delle rilevazioni possedute, in ordine alfabetico.
     *
     * Restituisce una COPIA e non la lista interna: chi la riceve la scorre fuori dal monitor, e se scorresse quella vera 
     * un add o un remove concorrente la modificherebbe durante l'iterazione, facendo fallire il ciclo.
     */
    public synchronized List<String> elenco() {
        return new ArrayList<>(nomi);
    }
 
    /** Risponde dall'indice in memoria, senza toccare il disco. */
    public synchronized boolean contiene(String nome) {
        return nomi.contains(nome);
    }
 
    /**
     * Il contenuto grezzo della rilevazione, pronto per essere spedito come payload di una FETCH. 
     * Byte e non String: il file viaggia sul socket cosi' com'e', e una conversione in mezzo cambierebbe la lunghezza dichiarata da LENGTH.
     */
    public synchronized byte[] leggi(String nome) throws IOException {
        File file = new File(cartella, nome + ESTENSIONE);
        return Files.readAllBytes(file.toPath());
    }
 
    /**
     * Aggiunge una rilevazione: scrive il file e poi aggiorna l'indice.
     *
     * Un nome gia' presente NON viene sovrascritto: le rilevazioni di un nodo hanno nome univoco, e una sovrascrittura silenziosa 
     * perderebbe un dato senza che nessuno se ne accorga.
     */
    public synchronized boolean aggiungi(String nome, byte[] contenuto) {
        if (nomi.contains(nome)) {
            return false;
        }
 
        File file = new File(cartella, nome + ESTENSIONE);
        try {
            Files.write(file.toPath(), contenuto);
        } catch (IOException e) {
            return false;
        }
 
        nomi.add(nome);
        Collections.sort(nomi);
        return true;
    }
 
    /**
     * Rimuove una rilevazione: cancella il file e poi aggiorna l'indice.
     */
    public synchronized boolean rimuovi(String nome) {
        if (!nomi.contains(nome)) {
            return false;
        }
 
        File file = new File(cartella, nome + ESTENSIONE);
        if (!file.delete()) {
            return false;
        }
 
        nomi.remove(nome);
        return true;
    }
}