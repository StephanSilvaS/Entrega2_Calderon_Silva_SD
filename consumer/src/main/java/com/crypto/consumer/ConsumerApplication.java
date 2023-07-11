package com.crypto.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

//@SpringBootApplication
public class ConsumerApplication {

	public static void main(String[] args) {
		// SpringApplication.run(ConsumerApplication.class, args);
		ConsumerService consumerService = new ConsumerService();
		consumerService.recibirMensajes();
	}

}
