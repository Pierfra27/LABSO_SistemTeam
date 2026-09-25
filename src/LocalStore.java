import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

//contiene i dati di un nodo specifico 
public class LocalStore {
    private final File cartella;
     /** I nomi delle rilevazioni possedute, senza estensione, tenuti ordinati. */
    private final List<String> nomi = new ArrayList<>();

    /** Estensione dei file dell'archivio: il nome della rilevazione e' il resto. */
    private static final String ESTENSIONE = ".txt";
 /**
     * Apre l'archivio e costruisce l'indice leggendo la cartella.
     *
     * @throws IOException se il percorso non esiste, non e' una cartella o non e' leggibile.
     *         Sono casi in cui il nodo non deve partire: si accorgerebbe dell'errore solo
     *         piu' tardi, con un listdata local vuoto e nessuna spiegazione. Una cartella che
     *         esiste ma e' vuota NON e' un errore: si parte con zero rilevazioni, ed e' un
     *         caso legittimo previsto dal piano.
     */
    public LocalStore(String percorsoCartella) {
        this.cartella = new File(percorsoCartella);
       
        
        File[] contenuto = cartella.listFiles();

    if (contenuto != null) {
        for (File f : contenuto) {
            if (f.isFile()) {
                String nomeFile = f.getName();              // es: "press_rn.txt"
                String nomeRilevazione = nomeFile.substring(0, nomeFile.length() - 4);
                nomi.add(nomeRilevazione);
                //nomi è una lista di stringhe 
            }
        }
        Collections.sort(nomi);
         }
    }
 /**
     * I nomi delle rilevazioni possedute, in ordine alfabetico.
     *
     * Restituisce una COPIA e non la lista interna: chi la riceve la scorre fuori dal
     * monitor, e se scorresse quella vera un add o un remove concorrente la modificherebbe
     * durante l'iterazione, facendo fallire il ciclo con una ConcurrentModificationException.
     */
    public synchronized List<String> elenco() {
        //  ritornare una COPIA della lista, ordinata
        //ritorni una copia solo se fai riferimento a uno stato che l'oggettp continua a possedere e riusare
        return new ArrayList<>(nomi);
    }

    public synchronized boolean contiene(String nome) {
        //scorre la lista di stringhe e trova se una di quelle stringhe è uguale a nome
       return nomi.contains(nome);
       //contains non tocca mai il disco 
    }
 /**
     * Il contenuto grezzo della rilevazione, pronto per essere spedito come payload di una
     * FETCH. Byte e non String: il file viaggia sul socket cosi' com'e', e una conversione in
     * mezzo cambierebbe la lunghezza dichiarata da LENGTH.
     *
     * @throws IOException se il file non esiste o non e' leggibile; chi chiama la traduce in
     *         ERR NOTFOUND. Puo' accadere anche per un nome presente nell'indice, se qualcuno
     *         ha cancellato il file dal disco senza passare da qui: e' proprio il
     *         disallineamento per cui esiste il protocollo robusto di download.
     */
    public synchronized byte[] leggi(String nome) throws IOException {
        //  leggere il contenuto del file <nome>.txt
        //ritorna il contenuto grezzo dal disco rispetto il file che mi serve e torna una sequenza di byte
        File file = new File(cartella, nome + ESTENSIONE);
         return Files.readAllBytes(file.toPath());
    } 
    
 /**
     * Aggiunge una rilevazione: scrive il file e poi aggiorna l'indice.
     *
     * Un nome gia' presente NON viene sovrascritto: le rilevazioni di un nodo hanno nome
     * univoco, e una sovrascrittura silenziosa perderebbe un dato senza che nessuno se ne
     * accorga.
     *
     * @return true se la rilevazione e' stata aggiunta; false se il nome esisteva gia'
     *         oppure se la scrittura su disco e' fallita. In quest'ultimo caso l'indice non
     *         viene toccato, quindi archivio e disco restano allineati. Chi chiama puo'
     *         distinguere i due casi interrogando prima contiene().
     */
    public synchronized boolean aggiungi(String nome, byte[] contenuto) {
        //  scrivere il file, aggiornare l'indice
        if(nomi.contains(nome))
        {
            return false;
        }
        File file=new File(cartella,nome + ".txt");
            try {

            Files.write(file.toPath(), contenuto);//il dove (file.toPath)
                                                  //cosa: contenuto 
                                                  //write scrive su disco
  
            }catch (IOException e) {
            return false;   // scrittura fallita, non aggiorniamo l'indice
            }
        // ritorna false se il nome esiste già (niente sovrascritture silenziose)

        nomi.add(nome);
        Collections.sort(nomi);
        return true;
    }
/**
     * Rimuove una rilevazione: cancella il file e poi aggiorna l'indice.
     *
     * @return true se e' stata rimossa; false se il nome non era presente o se la
     *         cancellazione non e' riuscita. Anche qui l'indice si aggiorna solo dopo il
     *         successo sul disco: un nome tolto dall'indice con il file ancora presente
     *         renderebbe la rilevazione invisibile ma occupante.
     */
    public synchronized boolean rimuovi(String nome) {
        // cancellare il file, togliere dall'indice
        if(!nomi.contains(nome))
        {
            return false;  //non esiste niente da rimuovere
        }
        //creo un percorso che punta a file gia esistente 
          File file = new File(cartella, nome + ESTENSIONE);
       if(!file.delete())
       {
        return false; //cancellazione fallita , il delete non è andato
       }
       nomi.remove(nome);
       return true;

    }

}