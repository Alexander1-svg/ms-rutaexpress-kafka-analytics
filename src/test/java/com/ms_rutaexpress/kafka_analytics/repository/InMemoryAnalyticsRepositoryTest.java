package com.ms_rutaexpress.kafka_analytics.repository;

import com.ms_rutaexpress.kafka_analytics.config.KafkaAnalyticsProperties;
import com.ms_rutaexpress.kafka_analytics.event.ShipmentAnalyticsEventV1;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryAnalyticsRepositoryTest {

    @Test
    void countsStatusesIgnoresDuplicatesAndLimitsRecentEvents() {
        InMemoryAnalyticsRepository repository = repository(2);

        assertThat(repository.tryStart("source-1")).isTrue();
        repository.complete(event("metric-1", "source-1", "CREATED"));
        assertThat(repository.tryStart("source-1")).isFalse();

        assertThat(repository.tryStart("source-2")).isTrue();
        repository.complete(event("metric-2", "source-2", "IN_TRANSIT"));
        assertThat(repository.tryStart("source-3")).isTrue();
        repository.complete(event("metric-3", "source-3", "DELIVERED"));

        var snapshot = repository.snapshot();
        assertThat(snapshot.totalProcessed()).isEqualTo(3);
        assertThat(snapshot.duplicateEvents()).isEqualTo(1);
        assertThat(snapshot.byStatus()).containsEntry("CREATED", 1L)
                .containsEntry("IN_TRANSIT", 1L)
                .containsEntry("DELIVERED", 1L);
        assertThat(snapshot.recentEvents()).extracting(event -> event.eventId())
                .containsExactly("source-3", "source-2");
    }

    private InMemoryAnalyticsRepository repository(int recentLimit) {
        return new InMemoryAnalyticsRepository(new KafkaAnalyticsProperties(
                new KafkaAnalyticsProperties.Topics("shipments", "analytics", ".DLT"),
                "group", Duration.ofSeconds(5), Duration.ofSeconds(1), 2, recentLimit
        ));
    }

    private ShipmentAnalyticsEventV1 event(String eventId, String sourceEventId, String status) {
        return new ShipmentAnalyticsEventV1(
                "1.0", eventId, sourceEventId, "ENV-10", "SHIPMENT_STATUS_OBSERVED", status,
                Instant.parse("2026-11-15T20:30:00Z"), Instant.parse("2026-11-15T20:30:01Z"), null
        );
    }
}

