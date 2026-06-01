package br.com.souza.inventory_service.adapter.`in`.consumer.order

import br.com.souza.inventory_service.adapter.`in`.consumer.order.dto.OrderCreatedEvent
import br.com.souza.inventory_service.application.domain.model.ReserveStockCommand
import br.com.souza.inventory_service.application.ports.`in`.ReserveStockUseCase
import br.com.souza.inventory_service.infrastructure.observability.TraceContextExtractor
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.SpanKind
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

@Component
class OrderCreatedKafkaConsumer(
    private val reserveStock: ReserveStockUseCase,
    private val traceContextExtractor: TraceContextExtractor
) {

    private val logger = LoggerFactory.getLogger(OrderCreatedKafkaConsumer::class.java)
    private val tracer = GlobalOpenTelemetry.getTracer("inventory-service")

    @KafkaListener(topics = ["orders.created"], groupId = "inventory-service", containerFactory = "kafkaListenerContainerFactory")
    fun consume(
        @Payload event: OrderCreatedEvent,
        @Header(name = "traceparent", required = false) traceParent: String?
    ) {
        val extractedContext = traceContextExtractor.extractContext(traceParent)

        val span = tracer.spanBuilder("orders.created process")
            .setParent(extractedContext)
            .setSpanKind(SpanKind.CONSUMER)
            .startSpan()

        span.makeCurrent().use {
            try {
                logger.info("Received order created event", kv("order_id", event.orderId))

                val command = ReserveStockCommand(
                    orderId = event.orderId,
                    productId = event.productId,
                    quantity = event.quantity,
                    paymentType = event.paymentType,
                    createdAt = event.createdAt,
                    traceParent = traceParent
                )

                reserveStock.execute(command)
            } finally {
                span.end()
            }
        }
    }
}