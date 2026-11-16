package com.ms_rutaexpress.kafka_analytics;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = "spring.kafka.listener.auto-startup=false")
class KafkaAnalyticsApplicationTests {

	@Test
	void contextLoads() {
	}

}
