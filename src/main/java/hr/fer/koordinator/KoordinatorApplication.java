package hr.fer.koordinator;

import java.util.Scanner;

public class KoordinatorApplication {
	public static void main(String[] args) {
		KafkaKoordinator coordinator = new KafkaKoordinator();
		coordinator.start();

		Scanner scanner = new Scanner(System.in);
		while (true) {
			System.out.println("Unesite komandu (start/stop/exit):");
			String command = scanner.nextLine();
			if ("start".equalsIgnoreCase(command)) {
				coordinator.sendStartCommand();
			} else if ("stop".equalsIgnoreCase(command)) {
				coordinator.sendStopCommand();
			} else if ("exit".equalsIgnoreCase(command)) {
				coordinator.stop();
				break;
			} else {
				System.out.println("Nepoznata komanda!");
			}
		}
	}
}
