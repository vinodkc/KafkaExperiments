package com.cloudera.ConsumedTopicManager.controller.it;

import com.cloudera.ConsumedTopicManager.entities.Greeting;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.*;
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class HelloRestControllerTests {

    @Autowired
    TestRestTemplate template;

    @Test
    public void greetWithName() {

        Greeting greeting = template.getForObject("/rest?name=dolly", Greeting.class);
        assertEquals("Hello dolly !", greeting.getMessage());
    }
    @Test
    public  void greeWithoutName() {
        ResponseEntity<Greeting> entity = template.getForEntity("/rest", Greeting.class);
        assertEquals(HttpStatus.OK, entity.getStatusCode());
        assertEquals(MediaType.APPLICATION_JSON, entity.getHeaders().getContentType());
        Greeting greeting = entity.getBody();
        assertEquals("Hello World !", greeting.getMessage());

    }

}
