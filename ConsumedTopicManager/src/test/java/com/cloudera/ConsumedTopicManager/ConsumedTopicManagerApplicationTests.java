package com.cloudera.ConsumedTopicManager;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

import java.text.NumberFormat;
import java.util.stream.Stream;

@SpringBootTest
class ConsumedTopicManagerApplicationTests {

	@Autowired
	ApplicationContext ctx;

	@Autowired @Qualifier("getDefaultNumberFormat")
	NumberFormat nf;

	@Test
	void defaultCurreny() {
	double amount = 12.56;
		System.out.println("Default Currency : " + nf.format(amount));
	}

	@Test
	void germanCurreny() {
		double amount = 12.56;
		NumberFormat germannf = ctx.getBean("getGermanNumberFormat", NumberFormat.class);
		System.out.println("Default Currency : " + germannf.format(amount));
	}

	@Test
	void contextLoads() {
		System.out.println("Count of beans : "+ ctx.getBeanDefinitionCount());
		Stream.of(ctx.getBeanDefinitionNames()).forEach(i -> System.out.println(i));
	}

}
