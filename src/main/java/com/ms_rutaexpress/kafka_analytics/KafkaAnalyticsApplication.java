package com.ms_rutaexpress.kafka_analytics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class KafkaAnalyticsApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaAnalyticsApplication.class, args);
	}

}
