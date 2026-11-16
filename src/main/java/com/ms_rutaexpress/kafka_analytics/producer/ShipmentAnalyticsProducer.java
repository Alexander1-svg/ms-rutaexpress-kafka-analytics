package com.ms_rutaexpress.kafka_analytics.producer;

import com.ms_rutaexpress.kafka_analytics.config.KafkaAnalyticsProperties;
import com.ms_rutaexpress.kafka_analytics.event.ShipmentAnalyticsEventV1;
import org.springframework.stereotype.Component;

@Component
public class ShipmentAnalyticsProducer {

    private final KafkaEventProducer producer;
    private final KafkaAnalyticsProperties properties;

    public ShipmentAnalyticsProducer(KafkaEventProducer producer, KafkaAnalyticsProperties properties) {
        this.producer = producer;
        this.properties = properties;
    }

    public void publish(ShipmentAnalyticsEventV1 event) {
        producer.publish(properties.topics().analytics(), event.shipmentId(), event);
    }
}

