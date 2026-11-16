package com.ms_rutaexpress.kafka_analytics.config;

import com.ms_rutaexpress.kafka_analytics.exception.InvalidShipmentEventException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorConfiguration {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorConfiguration.class);

    @Bean
    CommonErrorHandler kafkaErrorHandler(
            KafkaTemplate<String, String> kafkaTemplate,
            KafkaAnalyticsProperties properties
    ) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> {
                    TopicPartition destination = new TopicPartition(
                            record.topic() + properties.topics().deadLetterSuffix(),
                            record.partition()
                    );
                    log.error(
                            "Enrutando evento a DLT: sourceTopic={}, partition={}, offset={}, errorType={}",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            exception.getClass().getSimpleName()
                    );
                    return destination;
                }
        );

        DefaultErrorHandler handler = new DefaultErrorHandler(
                recoverer,
                new FixedBackOff(
                        properties.retryInterval().toMillis(),
                        properties.retryMaxAttempts()
                )
        );
        handler.addNotRetryableExceptions(InvalidShipmentEventException.class);
        return handler;
    }
}
