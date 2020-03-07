package com.cloudera.ConsumedTopicManager.controllers;

import com.cloudera.ConsumedTopicManager.entities.Greeting;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HelloRestController {

    @GetMapping("/rest")
    public Greeting greet(
            @RequestParam(defaultValue = "World",required = false) String name) {
        return  new Greeting(String.format("Hello %s !", name));
    }
}
