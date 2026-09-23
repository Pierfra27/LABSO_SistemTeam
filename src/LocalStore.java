import java.io.*;
import java.util.*;
//contiene i dati di un nodo specifico 
public class LocalStore {
    private final File cartella;
    private final List<String> nomi;

    public LocalStore(String percorsoCartella) {
        this.cartella = new File(percorsoCartella);
        this.nomi = new ArrayList<>();
        
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

    public synchronized byte[] leggi(String nome) throws IOException {
        //  leggere il contenuto del file <nome>.txt
        //ritorna il contenuto grezzo dal disco rispetto il file che mi serve e torna una sequenza di byte
        File file = new File(cartella, nome + ".txt");
         return Files.readAllBytes(file.toPath());
    } 
    

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

    public synchronized boolean rimuovi(String nome) {
        // cancellare il file, togliere dall'indice
        if(!nomi.contains(nome))
        {
            return false;  //non esiste niente da rimuovere
        }
        //creo un percorso che punta a file gia esistente 
        File file= new File(cartella, nome + ".txt");
       if(!file.delete())
       {
        return false; //cancellazione fallita , il delete non è andato
       }
       nomi.remove(nome);
       return true;

    }

}