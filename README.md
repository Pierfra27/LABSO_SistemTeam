# LABSO — Rete di nodi sensore con aggregatore centrale

Progetto di Laboratorio di Sistemi Operativi, A.A. 2025-26.

## Compilare

Dalla cartella del progetto:

```bash
javac -d bin src/*.java
```

`src/*.java` passa al compilatore tutti i sorgenti insieme, necessario perche'
le classi si richiamano a vicenda. `-d bin` fa scrivere i file compilati
(`.class`) dentro `bin/` invece che accanto ai sorgenti;

## Eseguire

```bash
# terminale 1 - il nodo aggregatore
java -cp bin Aggregator 9000

# terminali 2, 3, 4 - i nodi sensore
java -cp bin Client 127.0.0.1 9000 data/nodeA
java -cp bin Client 127.0.0.1 9000 data/nodeB
java -cp bin Client 127.0.0.1 9000 data/nodeC
```

Il terzo parametro del client e' la cartella con le sue rilevazioni ed e'
opzionale: serve a far girare piu' nodi sulla stessa macchina, ognuno con
contenuti diversi.

## Comandi

**Nodo sensore:** `listdata local`, `listdata remote [<nome>]`,
`listdata peers`, `add <nome> <contenuto>`, `remove <nome>`,
`download <nome>`, `download <peer> <nome>`, `help`, `quit`

**Aggregatore:** `listdata`, `log`, `help`, `quit`

## Struttura

```
src/     i sorgenti .java (piatti, senza package)
data/    rilevazioni di esempio, una cartella per nodo
doc/     documentazione, diagrammi
bin/     output di compilazione (creata da javac, non versionata)
```
