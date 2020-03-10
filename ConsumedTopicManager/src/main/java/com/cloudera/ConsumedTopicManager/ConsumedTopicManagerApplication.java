package com.cloudera.ConsumedTopicManager;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.text.NumberFormat;
import java.util.Locale;

@SpringBootApplication
public class ConsumedTopicManagerApplication {

	public static void main(String[] args) {
		SpringApplication.run(ConsumedTopicManagerApplication.class, args);


	}
	@Bean
	public NumberFormat getDefaultNumberFormat() {
		return NumberFormat.getCurrencyInstance();
	}

	@Bean
	public NumberFormat getGermanNumberFormat() {
		return NumberFormat.getCurrencyInstance(Locale.GERMANY);
	}
}
