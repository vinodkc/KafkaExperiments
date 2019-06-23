package com.vkc;

import com.vkc.model.Purchase;
import com.vkc.model.PurchasePattern;
import com.vkc.model.RewardAccumulator;
import com.vkc.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
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
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }

    public static void main(String[] args) {

        if (args.length < 5) {
            System.out.println("Usage: java -jar  kafkastreamsDemo1-1.0-SNAPSHOT-jar-with-dependencies.jar <brokerurl> <transactions> <patternsTopic> <rewardsTopic> <purchasesTopic>");
            System.exit(0);
        }


        String sourceTopic = args[1];
        String patternsTopic = args[2];
        String rewardsTopic = args[3];
        String purchasesTopic = args[4];


        Properties props = getProperties(args);


        Serde<String> stringSerde = Serdes.String();
        Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Purchase> purchaseKStream = builder.stream(sourceTopic, Consumed.with(stringSerde, purchaseSerde))
                .mapValues(p -> Purchase.builder(p).maskCreditCard().build());


        processPurchasePatterns(patternsTopic, stringSerde, purchaseKStream);
        processRewards(rewardsTopic, stringSerde, purchaseKStream);
        processPurchases(purchasesTopic, stringSerde, purchaseSerde, purchaseKStream);



        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props);

        kafkaStreams.start();


        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }

    private static void processPurchases(String purchasesTopic, Serde<String> stringSerde, Serde<Purchase> purchaseSerde, KStream<String, Purchase> purchaseKStream) {
        purchaseKStream.print(Printed.<String, Purchase>toSysOut().withLabel(purchasesTopic)); //for debugging

        purchaseKStream.to(purchasesTopic, Produced.with(stringSerde,purchaseSerde));
    }

    private static void processRewards(String rewardsTopic, Serde<String> stringSerde, KStream<String, Purchase> purchaseKStream) {
        KStream<String, RewardAccumulator> rewardsKStream =
                purchaseKStream.mapValues(purchase ->
                        RewardAccumulator.builder(purchase).build());
     //   rewardsKStream.print(Printed.<String, RewardAccumulator>toSysOut().withLabel(rewardsTopic)); //for debugging

        // send to topic 'rewards'
        rewardsKStream.to(rewardsTopic,
                Produced.with(stringSerde, StreamsSerdes.RewardAccumulatorSerde()));
    }

    private static void processPurchasePatterns(String patternsTopic, Serde<String> stringSerde, KStream<String, Purchase> purchaseKStream) {
        KStream<String, PurchasePattern> patternKStream = purchaseKStream
                .mapValues(purchase -> PurchasePattern.builder(purchase).build());

        //patternKStream.print(Printed.<String, PurchasePattern>toSysOut().withLabel(patternsTopic)); //for debugging

        // send to topic patterns
        patternKStream.to(patternsTopic, Produced.with(stringSerde, StreamsSerdes.PurchasePatternSerde()));
    }
}
