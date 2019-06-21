package com.vkc;

import com.vkc.model.Purchase;
import com.vkc.model.PurchasePattern;
import com.vkc.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class KafkaStreamTopologyDemo {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamTopologyDemo.class);

    private static Properties getProperties(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "KafkaStreamTopologyDemo-Streams-Client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "store-purchases");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "store-Kafka-Streams-App");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,  args[0]);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: java -jar  kafkastreamsDemo1-1.0-SNAPSHOT-jar-with-dependencies.jar <brokerurl> <sourceTopic> <DestTopic>");
            System.exit(0);
        }


        String sourceTopic = args[1];
        String destTopic = args[2];


        Properties props =getProperties(args);






        Serde<String> stringSerde = Serdes.String();
        Serde<Purchase>  purchaseSerde = StreamsSerdes.PurchaseSerde();

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String,Purchase> purchaseKStream = builder.stream(sourceTopic, Consumed.with(stringSerde, purchaseSerde))
                .mapValues(p -> Purchase.builder(p).maskCreditCard().build());

        KStream<String, PurchasePattern> patternKStream = purchaseKStream
                .mapValues(purchase -> PurchasePattern.builder(purchase).build());





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
