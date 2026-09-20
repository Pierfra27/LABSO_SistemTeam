import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * AggregatorState = MECCANISMO 1 del progetto.
 * 
 * Questa classe tiene lo stato "vero" dell'aggregatore:
 * - chi possiede quale risorsa (la tabella "risorsa -> lista di peerId")
 * - chi l'anagrafica dei peer registrati (peerId -> host/porta/attivo)
 * - il contatore che genera peer0, peer1, peer2, ...
 * 
 * REGOLA 1:
 * TUTTI i metodi pubblici sono "synchronized" sullo STESSO monitor (l'istanza stessa di AggregatorState, ce 
 * n'e'una sola nell'aggregatore). 
 * 
 * Perche' basta questo per rispettare la specifica "ogni altra richiesta deve attendere finche' la tabella non e'
 * di nuovo disponibile"?
 * Perche' in Java un thread dentro un metodo synchronized di un oggetto tiene il monitor di quell'oggetto, e 
 * nessun altro thread puo' entrare in nessun metodo synchronized dello stesso oggetto, nemmeno uno di sola lettura.
 * Quindi, mentre un thread sta scrivendo, tutti gli altri restano bloccati in coda ad aspettare il monitor: e' una 
 * mutua esclusione totale, non un lock "scritture vs scritture" con letture libere (quello sarebbe un ReadWriteLock, vietato
 * dal regolamento e comunque sbagliato per questa specifica, perche' permetterebbe di leggere la tabella mentre qualcuno la sta ancora
 * aggiornando).
 * 
 * REGOLA 2:
 * I meotodi di questa classe fanno solo lavoro in memoria: leggono/scrivono le HashMap e ritornano. Non chiamano MAI un meotodo
 * di un altro oggetto synchronized (es. non chiamano DownloadLog o TokenManager) e non fanno mai I/O di rete (non scrivono su un socket).
 * Chi orchestra le cose in sequenza (prima la tabella, poi il log, poi il socket) e' il thread NodeHandler, non questa classe.
 * Questo e' cio' che garantisce che un thread non tenga mai due monitor insieme, e che nessun metodo di questa classe possa "restare bloccato
 * a lungo": ogni chiamata dura microsecondi.
 **/

public class AggregatorState{

    /**
     * Anagrafica di un peer registrato. E' una "static class" annidata qui
     * (non un file a se') perche' e' un dato che esiste solo al servizio 
     * della tabella: chi possiede la tabella possiede anche uil suo tipo di
     * supporto.
     */

    public static class PeerInfo{
        public final String peerId;
        public final String host;
        public final int porta;
        public boolean attivo;

        PeerInfo(String peerId, String host, int porta){
            this.peerId = peerId;
            this.host = host;
            this.porta = porta;
            this.attivo = true;
        }

        //Copia difensiva: chi riceve un PeerInfo da fuori non deve poter modificare
        //l'originale custodito dentro il monitor.

        PeerInfo copia(){
            PeerInfo p = new PeerInfo(peerId, host, porta);
            p.attivo = attivo;
            return p;
        }
    }

    /**
     * Risultato di risolvi(): un piccolo oggetto valore con SOLO dati, senza comportamento.
     * Serve a far uscire dal monitor l'esito della scelta del candidato,
     * cosi' il chiamante (NodeHandler) puo' usarlo FUORI dal monitor
     * della tabella (principio delle classi passive).
     * 
     * peerId == null significa "nessun candidato trovato": il chiamante in quel caso
     * NON dece acquisire alcun toker (vedi TokenManager).
     */

    public static class EsitoRisolvi{
        public final String peerId; //nulla se non c'e' candidato
        public final String host;
        public final int porta;
        
        EsitoRisolvi(String peerId, String host, int porta){
            this.peerId = peerId;
            this.host = host;
            this.porta = porta;
        }

        static EsitoRisolvi nessunCandidato(){
            return new EsitoRisolvi(null, null, 0);
        }
    }
}