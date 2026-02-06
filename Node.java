import java.io.*;// Importazione classi per I/O, networking, gestione thread e sincronizzazione
import java.net.*;// Importazione classi per networking (ServerSocket, Socket, InetSocketAddress)
import java.util.*;// Importazione classi per collezioni (List, ArrayList) e gestione eccezioni
import java.util.concurrent.*;// Importazione classi per gestione thread (ExecutorService, ScheduledExecutorService, ScheduledFuture) e sincronizzazione (AtomicBoolean)
import java.util.concurrent.atomic.*;// Importazione classi per gestione atomica di variabili (AtomicBoolean)

/**
    * Implementazione dell'algoritmo Bully per l'elezione del coordinatore in un ambiente distribuito.
    * Ogni processo è identificato da un ID univoco e comunica con gli altri tramite socket TCP.
    * Il processo con l'ID più alto che risponde diventa il coordinatore.
    * Il sistema gestisce dinamicamente la perdita del coordinatore e la rielezione.
    */
public class Node {

    // Configurazione nodi
    private final int processId;
    private static final int BASE_PORT = 5000;
    private static final int TOTAL_PROCESSES = 10;
    
    // Timeout configurabili
    private static final long WAIT_ANSWER_MS = 2500;
    private static final long WAIT_COORDINATOR_MS = 4500;
    private static final long CHECK_LEADER_INTERVAL = 3500;
    
    // Stato del processo
    private volatile Integer currentLeader = null;
    private volatile boolean inElectionPhase = false;
    private volatile boolean receivedAnswerFromHigher = false;
    
    // Thread management
    private final ExecutorService threadPool = Executors.newCachedThreadPool();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    
    // Per gestire timeout
    private ScheduledFuture<?> answerTimeoutTask = null;
    private ScheduledFuture<?> coordinatorTimeoutTask = null;

    public Node(int processId) {
        this.processId = processId;
    }

// Punto di ingresso del processo: verifica argomenti, inizializzazione nodo
    public static void main(String[] args) {
        if (args.length < 1) {// Verifica presenza argomento ID processo
            System.err.println("Errore: specificare l'ID del processo");//
            System.err.println("Sintassi: java Node <id>");
            System.exit(1);
        }// Parsing ID processo e validazione intervall
        
// In caso di errore, stampa messaggio e termina processo
        try {
            int processId = Integer.parseInt(args[0]);
            if (processId < 1 || processId > TOTAL_PROCESSES) {
                System.err.println("ID processo deve essere tra 1 e " + TOTAL_PROCESSES);
                System.exit(1);
            }

// Creazione e inizializzazione nodo            
            Node node = new Node(processId);
            node.initialize();
        } catch (NumberFormatException e) {
            System.err.println("ID processo non valido: " + args[0]);
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Errore: " + e.getMessage());
            System.exit(1);
        }
    }

// Inizializzazione del processo: avvio listener, attesa sincronizzazione, trigger elezione, monitoraggio leader
    private void initialize() throws Exception {
        log("Inizializzazione processo in corso...");
        
        // Avvio listener messaggi in ingresso
        threadPool.submit(this::startMessageListener);
        
        // Attesa avvio altri processi
        pauseExecution(8000);
    
        // Trigger elezione iniziale
        triggerElectionProcess();
        
        // Avvio monitoraggio leader
        scheduleLeaderHealthCheck();
        
        log("Processo pronto e operativo");
    }

// Listener per messaggi in ingresso: accetta connessioni, processa messaggi, gestisce eccezioni
    private void startMessageListener() {
        int port = calculatePort(processId);
        
        try (ServerSocket listener = new ServerSocket(port)) {
            log("Server attivo su porta " + port + " (localhost)");
            
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Socket connection = listener.accept();
                    threadPool.submit(() -> processIncomingMessage(connection));
                } catch (IOException e) {
                    if (!listener.isClosed()) {
                        log("Errore accettazione connessione: " + e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            log("*** Terminazione processo ***");
        }
    }

// Elaborazione messaggi in ingresso: parsing, gestione tipi messaggio, sincronizzazione stato
    private void processIncomingMessage(Socket connection) {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(connection.getInputStream()))) {
            
            String messageContent = reader.readLine();
            if (messageContent == null) return;
            
            String[] tokens = messageContent.split("\\|");
            if (tokens.length < 2) return;
            
            String messageType = tokens[0];
            int senderId = Integer.parseInt(tokens[1]);
            
            handleReceivedMessage(messageType, senderId);// Gestione messaggio in base al tipo: log, aggiornamento stato, trigger processi correlati
            
        } catch (IOException | NumberFormatException e) {
            // Silenzioso per evitare log spam
        } finally {
            closeQuietly(connection);
        }
    }

// Gestione messaggi ricevuti: log, risposta a ELECTION, gestione ANSWER e COORDINATOR, timeout per coordinatore
    private synchronized void handleReceivedMessage(String msgType, int sender) {
        switch (msgType) {
            case "ELECTION" -> {
                log("ELECTION ricevuto da P" + sender);// Se il mittente ha ID inferiore, rispondo con ANSWER e, se non sono già in elezione, triggero processo di elezione
                
                if (processId > sender) {
                    dispatchMessage(sender, "ANSWER");
// Se non sono già in fase di elezione, avvio processo di elezione per competere con il mittente
                    if (!inElectionPhase) {
                        threadPool.submit(this::triggerElectionProcess);
                    }
                }
            }

// Gestione messaggio ANSWER: log, aggiornamento stato, cancellazione timeout, attesa annuncio coordinatore            
            case "ANSWER" -> {
                log("ANSWER ricevuto da P" + sender);
                receivedAnswerFromHigher = true;
                
                if (answerTimeoutTask != null) {
                    answerTimeoutTask.cancel(false);
                }
                
                if (inElectionPhase) {
                    log("Attendo annuncio COORDINATOR...");
                    waitForCoordinatorAnnouncement();
                }//
            }

// Gestione messaggio COORDINATOR: log, aggiornamento coordinatore corrente, cancellazione timeout, log coordinatore attuale           
            case "COORDINATOR" -> {
                log("COORDINATOR da P" + sender);
                currentLeader = sender;
                inElectionPhase = false;
                
                if (coordinatorTimeoutTask != null) {
                    coordinatorTimeoutTask.cancel(false);
                }
                
                log(" Coordinatore = P" + sender);
            }// Gestione messaggio PING: risposta silenziosa per confermare raggiungibilità, senza log per evitare spam
            
            case "PING" -> {
                
            }
        }
    }

// Processo di elezione: verifica stato, invio ELECTION a processi superiori, gestione timeout per risposta e annuncio coordinatore
    private synchronized void triggerElectionProcess() {
        if (inElectionPhase) return;
        
        inElectionPhase = true;
        receivedAnswerFromHigher = false;
        
        log("Avvio elezione");
// Identifico processi con ID superiore e invio ELECTION       
        List<Integer> higherProcesses = new ArrayList<>();
        for (int i = processId + 1; i <= TOTAL_PROCESSES; i++) {
            higherProcesses.add(i);
        }
// Se nessun processo superiore risponde, mi dichiaro coordinatore       
        if (higherProcesses.isEmpty()) {
            log("Nessun nodo con ID superiore");
            declareAsCoordinator();
            return;
        }
// Invio ELECTION ai processi superiori e verifico raggiungibilità        
        boolean anyReachable = false;
        for (int targetId : higherProcesses) {
            if (dispatchMessage(targetId, "ELECTION")) {
                anyReachable = true;
            }// Se un processo superiore risponde, attendo risposta e annuncio coordinatore; altrimenti, se timeout scade senza risposta, mi dichiaro coordinatore
        }
        
        if (!anyReachable) {
            log("Nessun processo superiore raggiungibile");
            declareAsCoordinator();
            return;// Se i processi superiori esistono ma non rispondono, attendo il timeout per la dichiarazione coordinatore
        }

// Attendo risposta da processi superiori e gestisco timeout per eventuale dichiarazione coordinatore        
        answerTimeoutTask = scheduler.schedule(() -> {
            synchronized (this) {
                if (!receivedAnswerFromHigher && inElectionPhase) {
                    log("Timeout: nessun ANSWER ricevuto");
                    declareAsCoordinator();// Se timeout scade senza risposta, mi dichiaro coordinatore
                }
            }
        }, WAIT_ANSWER_MS, TimeUnit.MILLISECONDS);
    }

// Attesa annuncio coordinatore: se timeout scade senza annuncio, riavvio elezione
    private void waitForCoordinatorAnnouncement() {
        coordinatorTimeoutTask = scheduler.schedule(() -> {
            synchronized (this) {
                if (inElectionPhase) {
                    log("Timeout T': COORDINATOR non ricevuto, riavvio elezione");
                    inElectionPhase = false;
                    triggerElectionProcess();// Riavvio elezione se annuncio coordinatore non ricevuto entro timeout
                }
            }
        }, WAIT_COORDINATOR_MS, TimeUnit.MILLISECONDS);// Se annuncio coordinatore ricevuto, cancello timeout
    }

// Dichiarazione coordinatore: aggiornamento stato, log, invio COORDINATOR a processi inferiori
    private synchronized void declareAsCoordinator() {
        currentLeader = processId;
        inElectionPhase = false;
        
        log(" Mi dichiaro COORDINATORE ");
        
        for (int i = 1; i < processId; i++) {
            dispatchMessage(i, "COORDINATOR");
        }
    }

// Monitoraggio salute coordinatore: verifica periodica, se coordinatore non risponde, reset stato e trigger elezione
    private void scheduleLeaderHealthCheck() {
        scheduler.scheduleWithFixedDelay(() -> {
            Integer leader = currentLeader;
            
            if (leader != null && leader != processId) {
                if (!checkProcessHealth(leader)) {
                    log("Timeout: coordinatore P" + leader + " non risponde");// Reset stato e trigger elezione
                    currentLeader = null;
                    triggerElectionProcess();
                }
            }
        }, CHECK_LEADER_INTERVAL, CHECK_LEADER_INTERVAL, TimeUnit.MILLISECONDS);// Controllo salute coordinatore a intervalli regolari
    }

// Verifica salute processo target: tentativo di connessione, invio PING, gestione eccezioni per determinare raggiungibilità
    private boolean checkProcessHealth(int targetId) {
        try (Socket probe = new Socket()) {
            probe.connect(
                new InetSocketAddress("localhost", calculatePort(targetId)), 
                (int) WAIT_ANSWER_MS
            );// Se connessione stabilita, invio PING per confermare raggiungibilità
            
            PrintWriter writer = new PrintWriter(probe.getOutputStream(), true);
            writer.println("PING|" + processId);
            
            return true;
        } catch (IOException e) {
            return false;
        }
    }
// Invio messaggio a processo target: costruzione payload, tentativo di connessione, gestione eccezioni, log per messaggi non PING
    private boolean dispatchMessage(int destinationId, String msgType) {
        String payload = msgType + "|" + processId;
        
        try (Socket socket = new Socket()) {
            socket.connect(
                new InetSocketAddress("localhost", calculatePort(destinationId)),
                (int) WAIT_ANSWER_MS
            );// Se connessione stabilita, invio messaggio
            
            PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
            writer.println(payload);
            
            if (!msgType.equals("PING")) {
                log( msgType + " inviato a P" + destinationId);
            }
            return true;
            
        } catch (IOException e) {
            return false;
        }
    }
// Calcolo porta in base all'ID del processo: BASE_PORT + ID

    private int calculatePort(int id) {
        return BASE_PORT + id;
    }
// Utility per pausa esecuzione: gestione InterruptedException per evitare log spam e garantire corretta interruzione thread
    private void pauseExecution(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
// Utility per chiusura silenziosa di risorse: gestione IOException per evitare log spam
    private void closeQuietly(Closeable resource) {
        try {
            if (resource != null) resource.close();
        } catch (IOException ignored) {}
    }
// Utility per log: formato uniforme con identificativo processo, utile per debug e tracciamento flusso esecuzione
    private void log(String message) {
        System.out.println("[P" + processId + "] " + message);
    }//log con identificativo processo per tracciamento chiaro delle operazioni e stato del nodo
}// Fine implementazione algoritmo Bully per elezione del coordinatore in ambiente distribuito
