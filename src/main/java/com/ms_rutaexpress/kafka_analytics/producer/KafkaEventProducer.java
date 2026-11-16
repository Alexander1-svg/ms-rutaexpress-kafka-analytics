package com.ms_rutaexpress.kafka_analytics.producer;

import com.ms_rutaexpress.kafka_analytics.config.KafkaAnalyticsProperties;
import com.ms_rutaexpress.kafka_analytics.exception.KafkaPublishException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Component
public class KafkaEventProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaEventProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final KafkaAnalyticsProperties properties;

    public KafkaEventProducer(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            KafkaAnalyticsProperties properties
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    public void publish(String topic, String key, Object event) {
        Objects.requireNonNull(event, "event no puede ser nulo");
        if (topic == null || topic.isBlank() || key == null || key.isBlank()) {
            throw new IllegalArgumentException("Topic y key son obligatorios");
        }

        try {
            String payload = objectMapper.writeValueAsString(event);
            var result = kafkaTemplate.send(topic, key, payload)
                    .get(properties.publishTimeout().toMillis(), TimeUnit.MILLISECONDS);
            log.info(
                    "Evento Kafka publicado: topic={}, partition={}, offset={}",
                    topic,
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset()
            );
        } catch (JacksonException exception) {
            throw new KafkaPublishException("No fue posible serializar el evento Kafka", exception);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new KafkaPublishException("La publicación Kafka fue interrumpida", exception);
        } catch (ExecutionException | TimeoutException exception) {
            throw new KafkaPublishException("No fue posible publicar el evento Kafka", exception);
        }
    }
}

