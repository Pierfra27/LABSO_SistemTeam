import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

//controllo che i thread usino il socket uno alla volta, per l'intera durata dello 
//scambio richiesta-risposta
public class AggregatorLink {

    /**
     * Il risultato di uno scambio richiesta-risposta con l'aggregatore.
     * Solo dati, nessun comportamento (stesso pattern di EsitoRisolvi).
     */
    public static class Risposta {
        public final boolean ok;
        public final String[] campi;          // la prima riga, gia' spezzata
        public final List<String> righeExtra; // righe seguenti, per LIST/PEERS

        Risposta(boolean ok, String[] campi, List<String> righeExtra) {
            this.ok = ok;
            this.campi = campi;
            this.righeExtra = righeExtra;
        }
    }

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;
    private boolean linkCaduto = false;

    public AggregatorLink(Socket socket) throws IOException {
        this.socket = socket;
        this.in = socket.getInputStream();
        this.out = socket.getOutputStream();
        this.socket.setSoTimeout(10_000); // valore normale, 10 secondi?
    }
    //controllare che il link non sia già segnato come "caduto" , se lo è fallire subito
    // scrivere la richiesta sul socket , usando Protocol.writeLine
    // leggerla con Protocol.readLine
    // capire se è OK o ERR, se è Ok leggere le n righe succesive
    //impacchetare tutto in un oggetto Risposta e ritorniamo 
   public synchronized Risposta invia(String richiesta) {
    if (linkCaduto) {
        return new Risposta(false, new String[]{"ERR", "LINKCADUTO"}, new ArrayList<>());
    }

    try {
        Protocol.writeLine(out, richiesta);//protocol creato da noi che mette in contatto Client e Aggregator
        //qua mi assicuro che la stringa non contenga \n e li sostituisce con spazi
        //converte la stringa in byte e li scrive sul verso OUtputSream
        //scrive anche il byte del carattere\n 
        //chiama flush() per assicurarsi che i byte partano davvero subito

        
        //qua ci sono due casi per cui il rpocesso fallisce
        // o per l'eccezione lanciata da readLine o per la prima riga nulla
        } catch (IOException e) {
            linkCaduto = true;
            return new Risposta(false, new String[]{"ERR", "LINKCADUTO"}, new ArrayList<>());
        }
   
        return leggiRisposta();

 }

    /**
     * Manda la REGISTER: una riga di intestazione seguita da n righe (i nomi delle risorse).
     * Usato solo da Client.java, una volta sola, all'avvio.
     */
 public synchronized Risposta registrati(String richiesta, List<String> righeExtra) {
        if (linkCaduto) {
            return new Risposta(false, new String[]{"ERR", "LINKCADUTO"}, new ArrayList<>());
        }

        try {
            Protocol.writeLine(out, richiesta);
            for (int i = 0; i < righeExtra.size(); i++) {
                Protocol.writeLine(out, righeExtra.get(i));
            }
        } catch (IOException e) {
            linkCaduto = true;
            return new Risposta(false, new String[]{"ERR", "LINKCADUTO"}, new ArrayList<>());
        }

        return leggiRisposta();
    }
/**
     * Come invia(), ma con attesa INDEFINITA sulla lettura della risposta: usato SOLO per
     * RESOLVE e RESOLVE_AT, che possono restare senza risposta finche' il token del nodo
     * sorgente non si libera (Meccanismo 3). Il timeout normale viene ripristinato nel
     * finally, qualunque sia l'esito, cosi' nessun percorso di uscita lascia il socket con
     * il timeout sbagliato.
     */
  public synchronized Risposta inviaResolve(String richiesta) {
        try {
            socket.setSoTimeout(0);
            return invia(richiesta);
        } finally {
            try {
                socket.setSoTimeout(10_000);
            } catch (IOException e) {
                // il socket e' probabilmente gia' rotto: non c'e' altro da fare qui.
            }
        }
    }
    
     private Risposta leggiRisposta() {
        try {
            String primaRiga = Protocol.readLine(in);
            if (primaRiga == null) {
                linkCaduto = true;
                return new Risposta(false, new String[]{"ERR", "LINKCADUTO"}, new ArrayList<>());
            }

            String[] campi = Protocol.split(primaRiga);
            boolean ok = campi.length > 0 && campi[0].equals(Protocol.RESP_OK);

            List<String> righeExtra = new ArrayList<>();
            if (ok && campi.length >= 2) {
                try {
                    int n = Protocol.parseInt(campi[1]);
                    for (int i = 0; i < n; i++) {
                        String riga = Protocol.readLine(in);
                        if (riga == null) {
                            linkCaduto = true;
                            return new Risposta(false, new String[]{"ERR", "LINKCADUTO"}, new ArrayList<>());
                        }
                        righeExtra.add(riga);
                    }
                } catch (IOException e) {
                    // campi[1] non era un numero: risposta senza elenco (es. "OK peer3").
                }
            }

            return new Risposta(ok, campi, righeExtra);

        } catch (IOException e) {
            linkCaduto = true;
            return new Risposta(false, new String[]{"ERR", "LINKCADUTO"}, new ArrayList<>());
        }
    }

    public synchronized boolean linkCaduto() {
        return linkCaduto;
    }
}