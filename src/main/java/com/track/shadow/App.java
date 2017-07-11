package com.track.shadow;

public class App {
	public static void main(String[] args) {
		SynchronizedService service = new SynchronizedService();
		service.process();
	}
}
