package com.vkc;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;


public class ProducerWithAvroSchemaDemo
{
    public static final String USER_SCHEMA = "{"
            + "\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":["
            + "  { \"name\":\"str1\", \"type\":\"string\" },"
            + "  { \"name\":\"str2\", \"type\":\"string\" },"
            + "  { \"name\":\"int1\", \"type\":\"int\" }"
            + "]}";
    public static void main( String[] args )
    {
        // Check arguments length value
        if(args.length < 3){
            System.out.println("Enter topicname, bootstrapServers, message count ");
            return;
        }

        //Assign topicName to string variable
        String topicName = args[0];
        String bootstrapServers = args[1];
        int numMsgs = Integer.parseInt(args[2]);



        // create instance for properties to access producer configs
        Properties props = new Properties();

        props.put("bootstrap.servers", bootstrapServers);


        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer");

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(USER_SCHEMA);

        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

        Producer<String, byte[]> producer = new KafkaProducer<>(props);

        System.out.println("Started sending messages");
        for (int i = 0; i < numMsgs; i++) {

            GenericData.Record avroRecord = new GenericData.Record(schema);
            avroRecord.put("str1", "Str 1-" + i);
            avroRecord.put("str2", "Str 2-" + i);
            avroRecord.put("int1", i);

            byte[] bytes = recordInjection.apply(avroRecord);

            producer.send(new ProducerRecord<String, byte[]>(topicName, bytes));
            System.out.println("Sent msg " + i);

        }
        System.out.println("Message sent successfully");
        producer.close();
    }
}
