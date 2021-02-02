package org.orenn.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {

    public static void main(String[] args) {

        // create logger for the class
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

        String bootstrapServers = "127.0.0.1:9092";

        // create producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            // create a producer record
            String topic = "demo_topic";
            String value = "message from ProducerDemoWithKeys " + i;
            String key = "id_" + i;

            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topic, key, value);

            // send data - async
            kafkaProducer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully sent or exception is thrown
                    if (e == null) {
                        // the record was successfully sent
                        logger.info("received new metadata.\n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Key: " + key + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            });
        }

        // flush data
        kafkaProducer.flush();

        // flush and close producer
        kafkaProducer.close();
    }
}
