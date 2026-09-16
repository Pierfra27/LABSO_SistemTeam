import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Contratto di comunicazione fra Client e Aggregator. Sono processi separati:
 * possono solo scambiarsi byte su un socket, e qui e' definito che cosa quei byte significano.
 *
 * Formato: testo UTF-8, una riga per messaggio terminata da '\n', campi separati da spazio. 
 * Ogni risposta inizia con OK oppure con ERR <codice>.
 * Gli elenchi sono "OK <n>" seguito da n righe; i contenuti sono "LENGTH <n>" seguito da esattamente n byte.
 *
 * Regola: mai un BufferedReader su uno stream di socket. Il suo buffer tratterrebbe byte oltre la riga letta e
 * corromperebbe il messaggio della FETCH, che segue immediatamente una riga di controllo. 
 * BufferedReader e' ammesso solo su System.in, nelle due console.
 *
 * La classe non ha stato: solo metodi statici, che lavorano su parametri e variabili locali nello stack del chiamante. 
 * Per questo e' usabile da tutti i thread contemporaneamente senza alcuna sincronizzazione.
 */

public final class Protocol {

    // Servono a trasformare in errori gestibili alcuni casi che farebbero morire il processo:
    // una riga senza '\n' finale, un payload troppo grande, un campo numerico non valido.

    public static final int MAX_LINE_BYTES = 8 * 1024;

    public static final int MAX_PAYLOAD_BYTES = 10 * 1024 * 1024;

    public static final String REQ_REGISTER = "REGISTER";

    public static final String REQ_LIST = "LIST";

    public static final String REQ_PEERS = "PEERS";

    public static final String REQ_ADDED = "ADDED";

    public static final String REQ_REMOVED = "REMOVED";

    public static final String REQ_RESOLVE = "RESOLVE";

    public static final String REQ_RESOLVE_AT = "RESOLVE_AT";

    public static final String REQ_RELEASE = "RELEASE";

    public static final String REQ_DISCONNECT = "DISCONNECT";

    public static final String REQ_FETCH = "FETCH";

    public static final String RESP_OK = "OK";

    public static final String RESP_ERR = "ERR";

    public static final String RESP_LENGTH = "LENGTH";

    public static final String ERR_BADREQUEST = "BADREQUEST";

    public static final String ERR_NOTREGISTERED = "NOTREGISTERED";

    public static final String ERR_NOTFOUND = "NOTFOUND";

    public static final String ERR_BADTOKEN = "BADTOKEN";

    // Segnaposto per un argomento assente. I campi sono posizionali: lasciarli vuoti sposterebbe tutti quelli che seguono.

    public static final String NONE = "-";

    public static final String OUTCOME_OK = "OK";

    public static final String OUTCOME_FAIL = "FAIL";

    private Protocol() { }

    /**
     * Legge una riga di controllo byte a byte: si ferma al primo '\n', cosi' l'eventuale payload che segue resta intatto per readFully.
     * I byte si decodificano in UTF-8 solo alla fine.
     *
     * @return la riga senza terminatore, oppure null se l'altro capo ha chiuso prima di inviare qualsiasi byte (fine normale del dialogo)
     * @throws EOFException se lo stream finisce a meta' riga: messaggio troncato
     * @throws IOException  se la riga supera MAX_LINE_BYTES
     */

    public static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int b = in.read();
        if (b == -1) {
            return null;
        }
        while (b != -1 && b != '\n') {
            buffer.write(b);
            // il controllo sta dentro il ciclo, non dopo: un mittente che non invia mai farebbe crescere il buffer all'infinito 
            if (buffer.size() > MAX_LINE_BYTES) {
                throw new IOException("riga di controllo oltre " + MAX_LINE_BYTES + " byte");
            }
            b = in.read();
        }
        if (b == -1) {
            throw new EOFException("stream chiuso a meta' riga");
        }
        byte[] bytes = buffer.toByteArray();
        int length = bytes.length;
        // terminatore di riga Win: senza questo il '\r' finirebbe dentro l'ultimo campo e farebbe fallire i confronti 
        if (length > 0 && bytes[length - 1] == '\r') {
            length--;
        }
        return new String(bytes, 0, length, StandardCharsets.UTF_8);
    }

    /**
     * Scrive una riga di controllo e la manda subito. Il flush non e' opzionale: senza, i byte possono restare nel buffer di uscita mentre i
     * due processi si aspettano a vicenda, senza che venga sollevato alcun errore.
     *
     * '\r' e '\n' dentro il testo diventano spazi: una riga di controllo che li contenesse verrebbe letta dall'altra parte come due messaggi distinti e
     * sfaserebbe l'intero dialogo.
     */

    public static void writeLine(OutputStream out, String line) throws IOException {
        String safe = (line == null) ? "" : line.replace('\r', ' ').replace('\n', ' ');
        out.write(safe.getBytes(StandardCharsets.UTF_8));
        out.write('\n');
        out.flush();
    }

    /**
     * Scrive "LENGTH <n>" e subito dopo gli n byte del contenuto.
     *
     * L'intestazione la scrive questo metodo e non il chiamante: avendo l'array in mano conosce la lunghezza esatta, quindi l'invariante 
     * "dopo LENGTH n arrivano esattamente n byte" non dipende dalla disciplina di chi chiama.
     *
     * La lunghezza e' dichiarata in anticipo invece di usare un terminatore testuale (una riga END) perche' un terminatore si romperebbe il giorno in
     * cui una rilevazione contiene quella parola.
     */

    public static void writePayload(OutputStream out, byte[] payload) throws IOException {
        String intestazione = RESP_LENGTH + " " + payload.length + "\n";
        out.write(intestazione.getBytes(StandardCharsets.UTF_8));
        out.write(payload);
        // un solo flush alla fine: intestazione e byte partono insieme
        out.flush();
    }

    /**
     * Legge esattamente n byte, l'unico modo corretto di consumare un payload annunciato da "LENGTH n". Un singolo read() su un socket puo' restituirne
     * meno di quanti richiesti anche se il mittente li ha inviati tutti, perche' TCP e' un flusso e i dati arrivano spezzettati: senza il ciclo i
     * trasferimenti risultano troncati quando la rete e' lenta o il file grande.
     *
     * @throws EOFException se lo stream finisce prima degli n byte
     */

    public static byte[] readFully(InputStream in, int n) throws IOException {
        if (n < 0 || n > MAX_PAYLOAD_BYTES) {
            throw new IOException("lunghezza payload non valida: " + n);
        }
        byte[] data = new byte[n];
        int letti = 0;
        while (letti < n) {
            int r = in.read(data, letti, n - letti);
            if (r == -1) {
                throw new EOFException("ricevuti " + letti + " byte su " + n);
            }
            letti += r;
        }
        return data;
    }

    /**
     * Converte un campo numerico del protocollo (una porta, un conteggio, una lunghezza). Esiste per trasformare la NumberFormatException, che non e'
     * controllata, in una IOException che lo e': cosi' il compilatore obbliga chi chiama a gestirla e a rispondere ERR BADREQUEST, invece di far
     * terminare il thread su una riga malformata.
     */

    public static int parseInt(String campo) throws IOException {
        try {
            return Integer.parseInt(campo);
        } catch (NumberFormatException e) {
            throw new IOException("campo numerico non valido: " + campo);
        }
    }

    /**
     * Spezza una riga in tutti i suoi campi. 
     * L'array puo' essere piu' corto del previsto: va sempre controllato con .length prima di essere indicizzato,
     * rispondendo ERR BADREQUEST se i campi non bastano.
     */
    public static String[] split(String line) {
        return split(line, 0);
    }

    /**
     * Come sopra, ma l'ultimo campo prende tutto il resto della riga, spazi interni compresi. 
     * Serve al contenuto di "add <nome> <contenuto>": i nomi delle rilevazioni si assumono senza spazi, il contenuto no.
     */

    public static String[] split(String line, int maxParts) {
        if (line == null) {
            return new String[0];
        }
        String trimmed = line.trim();
        if (trimmed.isEmpty()) {
            return new String[0];
        }
        // " +" e non " ": due spazi accidentali creerebbero un campo vuoto e sfaserebbero tutte le posizioni successive.
        return trimmed.split(" +", maxParts);
    }
}