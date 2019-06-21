package com.vkc;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;


public class SecureProducer
{
    public static void main( String[] args )
    {
        // Check arguments length value
        if(args.length < 3){
            System.out.println("Enter topicname, bootstrapServers, message count ");
            return;
        }

        //Assign topicName to string variable
        String topicName = args[0].toString();
        String bootstrapServers = args[1].toString();
        int numMsgs = Integer.parseInt(args[2]);

        // create instance for properties to access producer configs
        Properties props = new Properties();

        props.put("bootstrap.servers", bootstrapServers);


        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXTSASL");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        System.out.println("Started sending messages");
        for (int i = 0; i < numMsgs; i++) {
            producer.send(new ProducerRecord<String, String>(topicName,
                    Integer.toString(i), "Msg"+Integer.toString(i)));

        }
        System.out.println("Message sent successfully");
        producer.close();
    }
}
