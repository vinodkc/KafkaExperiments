package com.cloudera.ConsumedTopicManager.controller.ut;

import com.cloudera.ConsumedTopicManager.controllers.HelloController;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.ui.Model;
import org.springframework.validation.support.BindingAwareModelMap;

import static org.junit.Assert.*;
@SpringBootTest
public class HelloControllerUnitTest {
    @Test
    void sayHello() {
        HelloController controller = new HelloController();
        Model model = new BindingAwareModelMap();
        String result = controller.hello("world", model);
        assertEquals("hello", result);
        assertEquals("world", model.getAttribute("user"));
    }
}
