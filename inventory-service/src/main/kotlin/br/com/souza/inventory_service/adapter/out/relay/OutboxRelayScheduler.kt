package br.com.souza.inventory_service.adapter.out.relay

import br.com.souza.inventory_service.application.ports.out.OutboxEventRepositoryPort
import net.logstash.logback.argument.StructuredArguments.kv
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import java.nio.charset.StandardCharsets

@Component
class OutboxRelayScheduler(
    private val outboxRepository: OutboxEventRepositoryPort,
    private val kafkaTemplate: KafkaTemplate<String, String>
) {

    private val logger = LoggerFactory.getLogger(OutboxRelayScheduler::class.java)

    @Scheduled(fixedDelay = 1000)
    @Transactional
    fun relay() {
        val events = outboxRepository.findPendingEvents(50)
        if (events.isEmpty()) return

        val ids = events.mapNotNull { it.id }
        outboxRepository.lockEvents(ids)

        for (event in events) {
            val id = event.id ?: continue
            try {
                val key = "${event.aggregateType}:${event.aggregateId}"
                val record = ProducerRecord<String, String>(event.topic, key, event.payload)
                if (event.traceParent != null) {
                    record.headers().add("traceparent", event.traceParent.toByteArray(StandardCharsets.UTF_8))
                }

                kafkaTemplate.send(record).get()
                outboxRepository.markAsSent(id)
                logger.info("Outbox event published", kv("id", id), kv("topic", event.topic))
            } catch (ex: Exception) {
                logger.error("Failed to publish outbox event", kv("id", id), ex)
                if (event.retriesCount + 1 >= event.maxRetries) {
                    outboxRepository.markAsDeadLetter(id)
                    logger.warn("Outbox event moved to dead letter", kv("id", id))
                } else {
                    outboxRepository.markAsFailed(id)
                }
            }
        }
    }
}
