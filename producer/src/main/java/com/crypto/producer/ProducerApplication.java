package com.crypto.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import io.ably.lib.types.AblyException;

//@SpringBootApplication
public class ProducerApplication {

	public static void main(String[] args) throws AblyException {
		//SpringApplication.run(ProducerApplication.class, args);
		ProducerService producerService = new ProducerService();
		producerService.ablyAPI();
	}

}
