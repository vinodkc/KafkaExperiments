package com.vkc;


import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerWithAvroSchemaDemo2
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
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);

        //Kafka ConsumerDemo subscribes list of topics here.
        consumer.subscribe(Arrays.asList(topicName));

        //print the topic name
        System.out.println("Subscribed to topic " + topicName);

        Schema.Parser parser = new Schema.Parser();
        Schema schema = null;
        try {
            schema = parser.parse(
                    Thread.currentThread().getContextClassLoader().getResourceAsStream("./avro/LogRecord.avsc"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(1000);
            for (ConsumerRecord<String, byte[]> record: records) {
                String key = record.key();
                byte[] value = record.value();
                GenericRecord genericRecord = recordInjection.invert(value).get();
                System.out.println("ip= " + genericRecord.get("ip")
                        + ", timestamp= " + genericRecord.get("timestamp")
                        + ", url=" + genericRecord.get("url")
                        + ", referrer=" + genericRecord.get("referrer")
                        + ", useragent=" + genericRecord.get("useragent")
                        + ", sessionid=" + genericRecord.get("sessionid"));

            }
        }
    }
}
