package com.cloudera.ConsumedTopicManager.controllers;

import com.cloudera.ConsumedTopicManager.entities.Greeting;
import com.cloudera.ConsumedTopicManager.kafka.KafkaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaRestController {

    @Autowired
    KafkaService service;

    @GetMapping("/kafka")
    public Greeting kafkaConsume(
            @RequestParam(required = true) String topicName,
            @RequestParam(required = true) String bootstrapservers,
            @RequestParam(required = true) String groupname) {
        service.consumeMessages(topicName, bootstrapservers, groupname);
        return  new Greeting(String.format("Hello %s !", topicName));
    }

    @GetMapping("/rest")
    public Greeting greet(
            @RequestParam(defaultValue = "World",required = false) String name) {
        return  new Greeting(String.format("Hello %s !", name));
    }
}
