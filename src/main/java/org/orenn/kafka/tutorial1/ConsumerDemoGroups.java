package org.orenn.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoGroups {

    public static void main(String[] args) {

        /*
        For this demo start/stop multiple instances of this class and watch logs as ConsumerCoordinator re-balances
        partitions over group members. Also run the producer and watch how each consumer in group is polling for only
        its partition.
        Note that you must allow multiple instances in run configurations for this class,
        to do this:
        1. click on the dropdown list next to the play button on top right side of the bar
        2. choose edit configurations
        3. click on modify options
        4. select allow multiple instances
         */

        // create logger for the class
        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class.getName());

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "consumer-app-group-2";
        String topic = "demo_topic";

        // create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // subscribe consumer to topic(s)
        /*
        subscribe to single topic
        kafkaConsumer.subscribe(Collections.singleton(topic));
         */

        // subscribe to multiple topics using Array
        /*kafkaConsumer.subscribe(Arrays.asList("first_topic", "second_topic"));*/

        // subscribe to multiple topics using List
        List<String> topicList = new ArrayList<>();
        topicList.add(topic);
        topicList.add("second_topic");
        topicList.add("third_topic");

        kafkaConsumer.subscribe(topicList);

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }
    }
}
