package com.cloudera.ConsumedTopicManager.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Properties;

@Service
public class KafkaService {

    private Logger logger = LoggerFactory.getLogger(KafkaService.class);


    public void consumeMessages(String topicName,
                                String bootstrapservers,
                                String groupname) {

        //Kafka consumer configuration settings
        Properties props = new Properties();

        props.put("bootstrap.servers", bootstrapservers);
        props.put("group.id", groupname);
        props.put("enable.auto.commit", "false");
        // props.put("auto.commit.interval.ms", "1000");
        // props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        //Kafka ConsumerDemo subscribes list of topics here.
        consumer.subscribe(Arrays.asList(topicName));

        //print the topic name
        logger.info("Subscribed to topic " + topicName);
        int i = 0;
        while (i == 0) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)

                // print the offset,key and value for the consumer records.
                logger.info(String.format("count %d offset = %d, key = %s, value = %s",
                        ++i, record.offset(), record.key(), record.value()));
        }
        consumer.close();


    }
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Enter  topicname, bootstrapservers , groupname ");
            return;
        }

        //Kafka consumer configuration settings
        String topicName = args[0].toString();
        String bootstrapservers = args[1].toString();
        Properties props = new Properties();

        props.put("bootstrap.servers", bootstrapservers);
        props.put("group.id", args[2]);
        props.put("enable.auto.commit", "false");
       // props.put("auto.commit.interval.ms", "1000");
       // props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        //Kafka ConsumerDemo subscribes list of topics here.
        consumer.subscribe(Arrays.asList(topicName));

        //print the topic name
        System.out.println("Subscribed to topic " + topicName);
        int i = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {

                // print the offset,key and value for the consumer records.
                System.out.printf("count %d offset = %d, key = %s, value = %s\n",
                        ++i, record.offset(), record.key(), record.value());
            }
            consumer.commitSync();
        }
    }
}
