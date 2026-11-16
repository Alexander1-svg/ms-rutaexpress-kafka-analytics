package com.ms_rutaexpress.kafka_analytics.consumer;

import com.ms_rutaexpress.kafka_analytics.event.ShipmentEventV1;
import com.ms_rutaexpress.kafka_analytics.exception.InvalidShipmentEventException;
import com.ms_rutaexpress.kafka_analytics.service.ShipmentAnalyticsProcessor;
import com.ms_rutaexpress.kafka_analytics.service.ShipmentEventValidator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ShipmentEventConsumerTest {

    private final ObjectMapper objectMapper = mock(ObjectMapper.class);
    private final ShipmentEventValidator validator = mock(ShipmentEventValidator.class);
    private final ShipmentAnalyticsProcessor processor = mock(ShipmentAnalyticsProcessor.class);
    private final ShipmentEventConsumer consumer = new ShipmentEventConsumer(objectMapper, validator, processor);

    @Test
    void deserializesValidatesAndProcessesRecord() throws Exception {
        ShipmentEventV1 event = event();
        when(objectMapper.readValue("{valid-json}", ShipmentEventV1.class)).thenReturn(event);
        when(processor.process(event)).thenReturn(ShipmentAnalyticsProcessor.ProcessingResult.PROCESSED);

        consumer.consume(new ConsumerRecord<>("shipments", 1, 5L, "ENV-10", "{valid-json}"));

        verify(validator).validate(event);
        verify(processor).process(event);
    }

    @Test
    void rejectsEmptyPayload() {
        assertThatThrownBy(() -> consumer.consume(new ConsumerRecord<>("shipments", 0, 1L, "ENV-10", "")))
                .isInstanceOf(InvalidShipmentEventException.class)
                .hasMessage("Payload Kafka vacío");
    }

    private ShipmentEventV1 event() {
        return new ShipmentEventV1(
                "1.0", "evt-1", "SHIPMENT_STATUS_CHANGED", Instant.parse("2026-11-15T20:30:00Z"),
                "ENV-10", "IN_TRANSIT", null
        );
    }
}

