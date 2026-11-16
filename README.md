# ms-rutaexpress-kafka-analytics

Microservicio de RutaExpress que consume eventos de envíos desde Kafka, valida y deduplica los mensajes, publica eventos analíticos y expone estadísticas mediante una API REST.

## Tecnologías

- Java 21
- Spring Boot 4.1.2
- Spring for Apache Kafka
- Maven Wrapper
- Kafka embebido en modo KRaft para pruebas de integración

## Flujo

```text
rutaexpress.shipments.events.v1
              |
              v
    ShipmentEventConsumer
              |
       validación / DLT
              |
              v
 ShipmentAnalyticsProcessor
       |              |
       v              v
 métricas RAM   rutaexpress.shipments.analytics.v1
       |
       v
 GET /api/v1/analytics/*
```

Los datos analíticos y los identificadores usados para deduplicación se mantienen en memoria. Se reinician al reiniciar la aplicación.

## Contratos

Los contratos JSON `v1` y ejemplos están documentados en [docs/EVENT_CONTRACTS.md](docs/EVENT_CONTRACTS.md).

La key Kafka de los eventos de entrada y salida es `shipmentId`. El productor analítico genera un `eventId` determinista usando el `eventId` de origen.

## Configuración

| Variable | Valor local predeterminado | Descripción |
| --- | --- | --- |
| `SERVER_PORT` | `8091` | Puerto HTTP |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Brokers Kafka |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | Protocolo de seguridad |
| `KAFKA_TOPIC_SHIPMENTS` | `rutaexpress.shipments.events.v1` | Topic de entrada |
| `KAFKA_TOPIC_ANALYTICS` | `rutaexpress.shipments.analytics.v1` | Topic analítico |
| `KAFKA_DLT_SUFFIX` | `.DLT` | Sufijo de Dead Letter Topic |
| `KAFKA_CONSUMER_GROUP` | `rutaexpress-kafka-analytics-v1` | Grupo consumidor |
| `KAFKA_LISTENER_CONCURRENCY` | `1` | Consumidores concurrentes |
| `KAFKA_PUBLISH_TIMEOUT` | `10s` | Espera máxima del ACK del productor |
| `KAFKA_RETRY_INTERVAL` | `1s` | Intervalo entre reintentos |
| `KAFKA_RETRY_MAX_ATTEMPTS` | `2` | Reintentos antes de DLT |
| `ANALYTICS_RECENT_EVENTS_LIMIT` | `20` | Eventos recientes conservados |

Para una conexión administrada, las propiedades adicionales de Spring Kafka se pueden proporcionar como variables externas, por ejemplo `SPRING_KAFKA_PROPERTIES_SASL_MECHANISM` y `SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG`. Las credenciales nunca deben guardarse en el repositorio.

La infraestructura debe crear los topics de entrada, salida y DLT. Las DLT deben tener al menos las mismas particiones que sus topics de origen. Este microservicio no crea ni administra topics.

## API REST

- `GET /api/v1/analytics/summary`: total procesado, duplicados, conteo por estado y eventos recientes.
- `GET /api/v1/analytics/statuses`: conteo por estado.
- `GET /api/v1/analytics/recent?limit=20`: últimos eventos; `limit` acepta valores entre 1 y 100.
- `GET /actuator/health`: estado de la aplicación.

Ejemplo:

```powershell
Invoke-RestMethod http://localhost:8091/api/v1/analytics/summary
```

## Ejecución desde IntelliJ IDEA

1. Abrir `C:\Users\alexa\OneDrive\Documentos\kafka\kafka-analytics\pom.xml` como proyecto Maven.
2. Configurar el Project SDK en Java 21.
3. Esperar a que IntelliJ importe las dependencias Maven.
4. Levantar la infraestructura Kafka externa y crear los topics requeridos.
5. Definir las variables de entorno necesarias en la configuración de ejecución.
6. Ejecutar `KafkaAnalyticsApplication`.
7. Consultar la API en `http://localhost:8091/api/v1/analytics/summary`.

## Pruebas

Desde IntelliJ se puede ejecutar el directorio `src/test/java` o cada clase individualmente. Desde PowerShell:

```powershell
.\mvnw.cmd clean verify
```

Las pruebas no necesitan AWS ni un Kafka instalado. La integración inicia temporalmente un broker Kafka KRaft, comprueba el flujo completo y lo detiene al finalizar.

## Garantías y límites actuales

- El productor espera confirmación del broker con `acks=all` e idempotencia habilitada.
- Los offsets se confirman por registro después de que el listener termina correctamente.
- Los mensajes inválidos no se reintentan; se enrutan a DLT.
- Los fallos recuperables se reintentan antes de usar la DLT.
- La deduplicación es segura entre hilos dentro de una única instancia, pero no persiste tras reinicios ni se comparte entre réplicas.
- No se almacenan métricas en Oracle ni en otra base de datos.
