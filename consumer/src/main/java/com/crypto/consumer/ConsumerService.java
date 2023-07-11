package com.crypto.consumer;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.time.Duration;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import org.apache.kafka.common.serialization.StringDeserializer;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.session.Session;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class ConsumerService{

    private static final String TOPIC_NAME = "btc";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public void recibirMensajes(){
        // Configuración del consumidor
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "valores_de_Bitcoin");

        // Creación del consumidor
        Consumer<String, String> consumer = new KafkaConsumer<>(props);

        // Suscripción al tópico
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        
        // Consumo de mensajes
        try (CqlSession session = CqlSession.builder()
                .withKeyspace("crypto")
                .build()) {

            int i = 0;
            while (i < 10) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String valor = record.value();
                    System.out.println("Mensaje recibido: " + valor);
                    // Procesa el mensaje desde la API
                    

                    // Inserta datos dentro de Cassandra
                    Statement<?> statement = SimpleStatement.builder("INSERT INTO bitcoin (id,valor) VALUES (?, ?)")
                            .addPositionalValue(i)
                            .addPositionalValue(valor)
                            .build();
                    session.execute(statement);
                    i++;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }    
    } 
}