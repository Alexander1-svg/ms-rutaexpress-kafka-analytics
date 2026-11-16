package com.ms_rutaexpress.kafka_analytics.service;

import com.ms_rutaexpress.kafka_analytics.event.ShipmentEventV1;
import com.ms_rutaexpress.kafka_analytics.exception.InvalidShipmentEventException;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShipmentEventValidatorTest {

    private static final ValidatorFactory VALIDATOR_FACTORY = Validation.buildDefaultValidatorFactory();
    private final ShipmentEventValidator validator = new ShipmentEventValidator(VALIDATOR_FACTORY.getValidator());

    @AfterAll
    static void closeValidatorFactory() {
        VALIDATOR_FACTORY.close();
    }

    @Test
    void acceptsValidVersionOneEvent() {
        assertThatCode(() -> validator.validate(validEvent())).doesNotThrowAnyException();
    }

    @Test
    void rejectsBlankRequiredFields() {
        ShipmentEventV1 event = new ShipmentEventV1(
                "1.0", "", "SHIPMENT_STATUS_CHANGED", Instant.parse("2026-11-15T20:30:00Z"),
                "ENV-10", "", null
        );

        assertThatThrownBy(() -> validator.validate(event))
                .isInstanceOf(InvalidShipmentEventException.class)
                .hasMessageContaining("eventId")
                .hasMessageContaining("status");
    }

    @Test
    void rejectsUnsupportedContractVersion() {
        ShipmentEventV1 event = new ShipmentEventV1(
                "2.0", "evt-1", "SHIPMENT_STATUS_CHANGED", Instant.parse("2026-11-15T20:30:00Z"),
                "ENV-10", "IN_TRANSIT", null
        );

        assertThatThrownBy(() -> validator.validate(event))
                .isInstanceOf(InvalidShipmentEventException.class)
                .hasMessage("Versión de contrato no soportada");
    }

    private ShipmentEventV1 validEvent() {
        return new ShipmentEventV1(
                "1.0", "evt-1", "SHIPMENT_STATUS_CHANGED", Instant.parse("2026-11-15T20:30:00Z"),
                "ENV-10", "IN_TRANSIT", "corr-1"
        );
    }
}

