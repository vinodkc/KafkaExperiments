package com.vkc;


import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo
{
    public static void main( String[] args ) {

        if(args.length < 2){
            System.out.println("Enter bootstrapservers & topic name  ");
            return;
        }
        //Kafka consumer configuration settings
        String topicName = args[0].toString();
        String bootstrapservers = args[1].toString();
        Properties props = new Properties();

        props.put("bootstrap.servers", bootstrapservers);
        props.put("group.id", "ConsumerDemo");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
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
            int i =0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)

                // print the offset,key and value for the consumer records.
                System.out.printf("count %d offset = %d, key = %s, value = %s\n",
                        ++i, record.offset(), record.key(), record.value());
        }
    }
}
