package com.ms_rutaexpress.kafka_analytics.service;

import com.ms_rutaexpress.kafka_analytics.event.ShipmentEventV1;
import com.ms_rutaexpress.kafka_analytics.exception.InvalidShipmentEventException;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.stream.Collectors;

@Service
public class ShipmentEventValidator {

    private final Validator validator;

    public ShipmentEventValidator(Validator validator) {
        this.validator = validator;
    }

    public void validate(ShipmentEventV1 event) {
        if (event == null) {
            throw new InvalidShipmentEventException("El evento no puede ser nulo");
        }

        Set<ConstraintViolation<ShipmentEventV1>> violations = validator.validate(event);
        if (!violations.isEmpty()) {
            String fields = violations.stream()
                    .map(violation -> violation.getPropertyPath().toString())
                    .sorted()
                    .distinct()
                    .collect(Collectors.joining(", "));
            throw new InvalidShipmentEventException("Campos inválidos: " + fields);
        }

        if (!ShipmentEventV1.SCHEMA_VERSION.equals(event.schemaVersion())) {
            throw new InvalidShipmentEventException("Versión de contrato no soportada");
        }
    }
}

