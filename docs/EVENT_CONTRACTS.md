# Contratos de eventos RutaExpress

## Evento de entrada `ShipmentEventV1`

Topic configurable mediante `KAFKA_TOPIC_SHIPMENTS`. La key Kafka debe ser `shipmentId` para mantener el orden de los eventos de un envío dentro de una partición.

```json
{
  "schemaVersion": "1.0",
  "eventId": "evt-550e8400-e29b-41d4-a716-446655440000",
  "eventType": "SHIPMENT_STATUS_CHANGED",
  "occurredAt": "2026-11-15T20:30:00Z",
  "shipmentId": "ENV-100045",
  "status": "IN_TRANSIT",
  "correlationId": "corr-79dc687c-fd2a-4b17-9544-ec86f30cc265"
}
```

`eventType` y `status` se validan como textos no vacíos, pero no se restringen a un catálogo local. El catálogo definitivo debe ser compartido por el microservicio dueño de envíos.

## Evento de salida `ShipmentAnalyticsEventV1`

Topic configurable mediante `KAFKA_TOPIC_ANALYTICS`. La key Kafka es `shipmentId`.

```json
{
  "schemaVersion": "1.0",
  "eventId": "evt-550e8400-e29b-41d4-a716-446655440000:shipment-status-observed:v1",
  "sourceEventId": "evt-550e8400-e29b-41d4-a716-446655440000",
  "shipmentId": "ENV-100045",
  "metricType": "SHIPMENT_STATUS_OBSERVED",
  "status": "IN_TRANSIT",
  "occurredAt": "2026-11-15T20:30:00Z",
  "processedAt": "2026-11-15T20:30:01Z",
  "correlationId": "corr-79dc687c-fd2a-4b17-9544-ec86f30cc265"
}
```

El `eventId` analítico es determinista a partir del evento de origen. Esto permite que consumidores posteriores también puedan deduplicarlo.

## Errores y DLT

Los mensajes con JSON inválido o campos inválidos no se reintentan. Los fallos transitorios se reintentan según `KAFKA_RETRY_INTERVAL` y `KAFKA_RETRY_MAX_ATTEMPTS`. Después se publican en `<topic-origen><KAFKA_DLT_SUFFIX>`.

Los topics, incluidas las DLT con igual número de particiones que su topic de origen, deben ser creados por la infraestructura externa. Este microservicio no usa Kafka Admin.
