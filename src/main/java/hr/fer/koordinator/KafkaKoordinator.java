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
            System.out.println("Ignorirana poruka poslana od Koordinatora: ");
            return;
        }
        System.out.println("Primljena registracija: " + message);
        String sensorId = extractSensorId(message);
        registeredSensors.put(sensorId, message);
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
        producerService.sendMessage("Command", "Start");
        System.out.println("Poruka 'Start' poslana svim senzorima.");

        registrationTimer = new Timer();
        registrationTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                broadcastRegistrations();
            }
        }, 8000);     }

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