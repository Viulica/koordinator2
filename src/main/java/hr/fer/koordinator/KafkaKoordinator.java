package hr.fer.koordinator;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class KafkaKoordinator {
    private final KafkaProducerService producerService;
    private final KafkaConsumerService consumerService;
    private Map<String, String> registeredSensors;
    private Timer registrationTimer;
    private volatile boolean isCollectingRegistrations = false;
    private static final long REGISTRATION_TIMEOUT = 3000; // 3 sekunde

    public KafkaKoordinator() {
        this.producerService = new KafkaProducerService();
        this.consumerService = new KafkaConsumerService("Register");
        this.registeredSensors = new HashMap<>();
    }

    public void start() {
        System.out.println("Koordinator pokrenut.");
        
        // Konfiguracija consumera za manual commit
        consumerService.startListening((topic, message) -> {
            try {
                if ("Register".equals(topic)) {
                    handleRegistration(message);
                }
            } catch (Exception e) {
                System.err.println("Greška pri obradi poruke: " + e.getMessage());
                // Ne commit-aj offset ako je došlo do greške
                return;
            }
            // Commit offset samo ako je obrada uspješna
        });
    }

    private void handleRegistration(String message) {
        if (!isCollectingRegistrations) {
            System.out.println("Ignorirana registracija jer nije aktivan period skupljanja");
            return;
        }

        if (message.contains("\"source\":\"coordinator\"")) {
            System.out.println("Ignorirana poruka poslana od Koordinatora");
            return;
        }

        try {
            System.out.println("Primljena registracija: " + message);
            String sensorId = extractSensorId(message);
            registeredSensors.put(sensorId, message);

            // Resetiraj timer jer smo primili novu registraciju
            if (registrationTimer != null) {
                registrationTimer.cancel();
            }
            scheduleRegistrationBroadcast();
        } catch (Exception e) {
            System.err.println("Greška pri obradi registracije: " + e.getMessage());
            throw e; // Propagiraj grešku da se ne commit-a offset
        }
    }

    private String extractSensorId(String message) {
        return message.split("\"id\":\"")[1].split("\"")[0];
    }

    private void broadcastRegistrations() {
        StringBuilder registrationsMessage = new StringBuilder("[");
        for (String registration : registeredSensors.values()) {
            registrationsMessage.append(registration).append(",");
        }
        if (registrationsMessage.length() > 1) {
            registrationsMessage.setLength(registrationsMessage.length() - 1);
        }
        registrationsMessage.append("]");
        String message = "{\"source\":\"coordinator\",\"registrations\":" + registrationsMessage + "}";

        producerService.sendMessage("Register", message);
        System.out.println("Poslane registracije svim čvorovima: " + message);
    }

    public void sendStartCommand() {
        registeredSensors.clear();
        if (registrationTimer != null) {
            registrationTimer.cancel();
        }
        
        isCollectingRegistrations = true;
        System.out.println("Započinjem period skupljanja registracija...");
        
        producerService.sendMessage("Command", "Start");
        System.out.println("Poruka 'Start' poslana svim senzorima.");

        // Inicijalni timer
        scheduleRegistrationBroadcast();
    }

    private void scheduleRegistrationBroadcast() {
        registrationTimer = new Timer();
        registrationTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    if (registeredSensors.isEmpty()) {
                        System.out.println("Nema registriranih senzora, čekam...");
                        return;
                    }
                    broadcastRegistrations();
                    isCollectingRegistrations = false;
                } catch (Exception e) {
                    System.err.println("Greška pri slanju registracija: " + e.getMessage());
                }
            }
        }, REGISTRATION_TIMEOUT);
    }

    public void sendStopCommand() {
        producerService.sendMessage("Command", "Stop");
        System.out.println("Poruka 'Stop' poslana svim senzorima.");
    }

    public void stop() {
        consumerService.close();
        producerService.close();
        System.out.println("Koordinator zaustavljen.");
    }
}