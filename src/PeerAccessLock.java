/**
 Questa classe gestisce l'accesso esclusivo al nodo sensore,
 ogni nodo puo' servire una sola richiesta FETCH(rilevazione) alla volta.
 Se un PeerHandler arriva mentre un altro sta gia' servendo
 una richiesta, rimane in attesa finche' il nodo non torna libero.
 */
public class PeerAccessLock {

    private boolean occupato = false;
    private String titolare = null;
    private int inAttesa = 0;
    /**
      Acquisisce il permesso di utilizzare il nodo.
      Se il nodo e' gia' occupato, il thread chiamante rimane
      in attesa fino al rilascio del permesso.
     */
    public synchronized void acquisisci(String richiedente)
            throws InterruptedException {
        inAttesa++;

        try {
            while (occupato) {
                wait(); //rimangono in attesa se =true
            }
        } finally {
            inAttesa--;
        }

        occupato = true;
        titolare = richiedente;
    }

    /**
     Rilascia il permesso e risveglia i PeerHandler
     che sono in attesa.
     */
    public synchronized void rilascia() {
        occupato = false;
        titolare = null;

        notifyAll(); //notifica tutti non uno solo
    }

    /**
     metodi test per capire se funziona tutto bene
     */
    public synchronized boolean isOccupato() {
        return occupato;
    }

    public synchronized String getTitolare() {
        return titolare;
    }

    public synchronized int getInAttesa() {
        return inAttesa;
    }
}