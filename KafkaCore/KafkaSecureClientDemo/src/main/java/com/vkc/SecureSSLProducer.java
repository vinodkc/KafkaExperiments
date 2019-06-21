package com.vkc;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;


public class SecureSSLProducer
{
    public static void main( String[] args )
    {
        // Check arguments length value
        if(args.length < 5){
            System.out.println("Enter topicname, bootstrapServers, message count, ssl.truststore.location, ssl.truststore.password ");
            return;
        }

        //Assign topicName to string variable
        String topicName = args[0].toString();
        String bootstrapServers = args[1].toString();
        int numMsgs = Integer.parseInt(args[2]);
        String ssl_truststore_location = args[3].toString();
        String ssl_truststore_password = args[4].toString();

        // create instance for properties to access producer configs
        Properties props = new Properties();

        props.put("bootstrap.servers", bootstrapServers);


        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put("ssl.truststore.location",ssl_truststore_location);
        props.put("ssl.truststore.password",ssl_truststore_password);

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        System.out.println("SASL SSL Producer Started sending messages");
        for (int i = 0; i < numMsgs; i++) {
            producer.send(new ProducerRecord<String, String>(topicName,
                    Integer.toString(i), "Msg"+Integer.toString(i)));

        }
        System.out.println("Message sent successfully");
        producer.close();
    }
}
