package com.vkc;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;


public class KafkaStreamDemo {
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: java -jar  kafkastreamsDemo1-1.0-SNAPSHOT-jar-with-dependencies.jar <brokerurl> <sourceTopic> <DestTopic>");
            System.exit(0);
        }

        String brokerurl = args[0];
        String sourceTopic = args[1];
        String destTopic = args[2];


        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "KafkaStreamDemo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerurl);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(sourceTopic, Consumed.with(stringSerde, stringSerde))
                .mapValues(x -> x.toUpperCase())
                .to(destTopic, Produced.with(stringSerde, stringSerde));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props);

        kafkaStreams.start();


        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        // print the topology every 10 seconds for debugging
        while (true) {
            System.out.println(kafkaStreams.toString());
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                break;
            }
        }

    }
}
