package com.ms_rutaexpress.kafka_analytics.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Validated
@ConfigurationProperties(prefix = "app.kafka")
public record KafkaAnalyticsProperties(
        @Valid @NotNull Topics topics,
        @NotBlank String consumerGroup,
        @NotNull Duration publishTimeout,
        @NotNull Duration retryInterval,
        @Min(0) long retryMaxAttempts,
        @Min(1) int recentEventsLimit
) {
    public record Topics(
            @NotBlank String shipments,
            @NotBlank String analytics,
            @NotBlank String deadLetterSuffix
    ) {
    }
}

