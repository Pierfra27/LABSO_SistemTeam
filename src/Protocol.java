import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Contratto comune fra nodo sensore e aggregatore: costanti dei messaggi e
 * primitive di I/O del protocollo.
 */

public final class Protocol {

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

    public static final String NONE = "-";

    public static final String OUTCOME_OK = "OK";

    public static final String OUTCOME_FAIL = "FAIL";

    private Protocol() { }}

    