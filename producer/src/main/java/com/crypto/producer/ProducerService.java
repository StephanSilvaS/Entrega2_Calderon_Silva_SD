package com.crypto.producer;

import io.ably.lib.realtime.AblyRealtime;
import io.ably.lib.types.AblyException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ProducerService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static final String TOPIC_NAME = "btc";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public void ablyAPI() throws AblyException{

        // Configuración del productor
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    
        // Creación del productor
        Producer<String, String> producer = new KafkaProducer<>(props);
        
        // Configurar la conexión
        String apiKey = "KMNnHQ.DbzYqQ:Ur6_rOy3cYjKvlazKcIiHNCSW_QkhjtmtCm4sFZ-J98";
        AblyRealtime ably = new AblyRealtime(apiKey);

        // Obtener un canal
        String channelName = "[product:ably-coindesk/crypto-pricing]btc:usd";
        io.ably.lib.realtime.Channel channel = ably.channels.get(channelName);


        List<String> listaValores = new ArrayList<String>();

        try{
            channel.subscribe(message -> {
                String mensaje = message.data.toString();
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, mensaje);
                producer.send(record);
                System.out.println("Mensaje enviado con éxito a Kafka.");
                listaValores.add(mensaje);
                
                if(listaValores.size() == 10){
                    ably.close();
                    producer.close();
                }
                
                System.out.println(mensaje);
                
            });
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            
        }
    }
}
