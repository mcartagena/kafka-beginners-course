package com.github.mcartagena.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args)  {
        final String bootstrapServers = "192.168.100.27:9092";
        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        for (int index = 0; index < 10; index++) {
            // create a producer record
            final String topic = "first_topic";
            final String value = "Hello world " + index;
            final String key = "id_" + index;

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            logger.info("Key: " + key);
            // id_0 partition 1
            // id_1 partition 0
            // id_2 partition 2
            // id_3 partition 0
            // id_4 partition 2
            // id_5 partition 2
            // id_6 partition 0
            // id_7 partition 2
            // id_8 partition 1
            // id_9 partition 2

            // send data - asynchronous
            producer.send(record, (metadata, exception) -> {
                // execute every time a record is successfully sent or an exception is thrown
                if (exception == null) {
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
            }); //.get(); // block the .send() to make it synchronous - don't to this in prod
        }

        // flush data
        producer.flush();

        // flush and close producer
        producer.close();

    }
}
