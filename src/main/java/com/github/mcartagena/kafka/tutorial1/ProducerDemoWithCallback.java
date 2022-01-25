package com.github.mcartagena.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Properties;


public class ProducerDemoWithCallback {

    public static void main(String[] args) {
        final String bootstrapServers = "192.168.100.27:9092";
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        for (int index = 0; index < 10; index++) {
            // create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "From my mac..." + index);

            // send data
            producer.send(record, (metadata, exception) -> {
                // execute every time a record is successfully sent or an exception is thrown
                if (exception != null) {
                    // the record was succesfully sent
                    logger.info("Received new metadata. \n" +
                            "Topic: " + metadata.topic() + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Offset: " + metadata.offset() + "\n" +
                            "Timestamp: " + metadata.timestamp()
                    );
                } else {
                    logger.error("Error while producing " + metadata.toString());
                }
            });
        }

        // flush data
        producer.flush();

        // flush and close producer
        producer.close();
    }
}
