package com.ms_rutaexpress.kafka_analytics.producer;

import com.ms_rutaexpress.kafka_analytics.config.KafkaAnalyticsProperties;
import com.ms_rutaexpress.kafka_analytics.event.ShipmentEventV1;
import com.ms_rutaexpress.kafka_analytics.exception.KafkaPublishException;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaEventProducerTest {

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    private final ObjectMapper objectMapper = mock(ObjectMapper.class);
    private final KafkaEventProducer producer = new KafkaEventProducer(kafkaTemplate, objectMapper, properties());

    @Test
    void serializesAndPublishesUsingProvidedTopicAndKey() throws Exception {
        SendResult<String, String> result = mock(SendResult.class);
        RecordMetadata metadata = mock(RecordMetadata.class);
        when(result.getRecordMetadata()).thenReturn(metadata);
        when(metadata.partition()).thenReturn(2);
        when(metadata.offset()).thenReturn(15L);
        when(objectMapper.writeValueAsString(org.mockito.ArgumentMatchers.any())).thenReturn("{\"eventId\":\"evt-1\"}");
        when(kafkaTemplate.send("topic-a", "ENV-10", "{\"eventId\":\"evt-1\"}"))
                .thenReturn(CompletableFuture.completedFuture(result));

        producer.publish("topic-a", "ENV-10", event());

        verify(kafkaTemplate).send("topic-a", "ENV-10", "{\"eventId\":\"evt-1\"}");
    }

    @Test
    void wrapsBrokerFailure() throws Exception {
        when(objectMapper.writeValueAsString(org.mockito.ArgumentMatchers.any())).thenReturn("{}");
        CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalStateException("broker unavailable"));
        when(kafkaTemplate.send("topic-a", "ENV-10", "{}")).thenReturn(failed);

        assertThatThrownBy(() -> producer.publish("topic-a", "ENV-10", event()))
                .isInstanceOf(KafkaPublishException.class)
                .hasMessage("No fue posible publicar el evento Kafka");
    }

    private KafkaAnalyticsProperties properties() {
        return new KafkaAnalyticsProperties(
                new KafkaAnalyticsProperties.Topics("shipments", "analytics", ".DLT"),
                "group", Duration.ofSeconds(1), Duration.ofSeconds(1), 2, 20
        );
    }

    private ShipmentEventV1 event() {
        return new ShipmentEventV1(
                "1.0", "evt-1", "SHIPMENT_STATUS_CHANGED", Instant.parse("2026-11-15T20:30:00Z"),
                "ENV-10", "IN_TRANSIT", null
        );
    }
}

