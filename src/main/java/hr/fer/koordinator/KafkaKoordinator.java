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
    private boolean isCollectingRegistrations = false;
    private static final long WAIT_AFTER_LAST_REGISTRATION = 3000; 
    public KafkaKoordinator() {
        this.producerService = new KafkaProducerService();
        this.consumerService = new KafkaConsumerService("Register");
        this.registeredSensors = new HashMap<>();
    }

    public void start() {
        System.out.println("Koordinator pokrenut.");
        consumerService.startListening((topic, message) -> {
            if ("Register".equals(topic)) {
                handleRegistration(message);
            }
        });

    }

    private void handleRegistration(String message) {
        if (message.contains("\"source\":\"coordinator\"")) {
            System.out.println("Ignorirana poruka poslana od Koordinatora");
            return;
        }
        if (!isCollectingRegistrations) {
            System.out.println("Ignorirana registracija jer nije aktivan period skupljanja");
            return;
        }

        System.out.println("Primljena registracija: " + message);
        String sensorId = extractSensorId(message);
        registeredSensors.put(sensorId, message);

        if (registrationTimer != null) {
            registrationTimer.cancel();
        }
        scheduleRegistrationBroadcast();
    }

    private String extractSensorId(String message) {
        return message.split("\"id\":\"")[1].split("\"")[0];
    }

    private void scheduleRegistrationBroadcast() {
        registrationTimer = new Timer();
        registrationTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                broadcastRegistrations();
                isCollectingRegistrations = false;  // Završi s prikupljanjem
                registrationTimer.cancel();
            }
        }, WAIT_AFTER_LAST_REGISTRATION);
    }

    public void sendStartCommand() {
        registeredSensors.clear();
        if (registrationTimer != null) {
            registrationTimer.cancel();
        }
        
        isCollectingRegistrations = true;  // Počni prikupljati registracije
        producerService.sendMessage("Command", "Start");
        System.out.println("Poruka 'Start' poslana svim senzorima.");

        // Postavi inicijalni timer
        scheduleRegistrationBroadcast();
    }

    public void sendStopCommand() {
        isCollectingRegistrations = false;
        if (registrationTimer != null) {
            registrationTimer.cancel();
        }
        registeredSensors.clear();
        producerService.sendMessage("Command", "Stop");
        System.out.println("Poruka 'Stop' poslana svim senzorima.");
    }

    public void stop() {
        consumerService.close();
        producerService.close();
        System.out.println("Koordinator zaustavljen.");
    }

    private void broadcastRegistrations() {
        if (registeredSensors.isEmpty()) {
            System.out.println("Nema registriranih senzora.");
            return;
        }

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
}