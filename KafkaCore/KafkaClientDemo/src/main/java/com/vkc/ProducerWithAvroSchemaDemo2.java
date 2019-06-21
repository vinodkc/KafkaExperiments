package com.vkc;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import com.vkc.avro.LogRecord;
import com.vkc.datagenerator.EventGenerator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;


public class ProducerWithAvroSchemaDemo2
{

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
        Schema schema = null;
        try {
            schema = parser.parse(
                    Thread.currentThread().getContextClassLoader().getResourceAsStream("./avro/LogRecord.avsc"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

        Producer<String, byte[]> producer = new KafkaProducer<>(props);

        System.out.println("Started sending messages");
        //It is an inefficient way to send Avro schema along with data, instead try with schema registry
        for (int i = 0; i < numMsgs; i++) {
            GenericData.Record avroRecord = getRecord(schema);
            byte[] bytes = recordInjection.apply(avroRecord);
            producer.send(new ProducerRecord<String, byte[]>(topicName, bytes));
        }
        System.out.println("Message sent successfully");
        producer.close();
    }

    private static GenericData.Record getRecord(Schema schema) {
        GenericData.Record avroRecord = new GenericData.Record(schema);
        LogRecord record =  EventGenerator.getNext();
        avroRecord.put("ip", record.getIp());
        avroRecord.put("timestamp", record.getTimestamp());
        avroRecord.put("url", record.getUrl());
        avroRecord.put("referrer", record.getReferrer());
        avroRecord.put("useragent", record.getUseragent());
        avroRecord.put("sessionid", record.getSessionid());
        System.out.println(record);
        return avroRecord;
    }
}
