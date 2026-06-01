package br.com.souza.inventory_service.adapter.`in`.consumer.order

import br.com.souza.inventory_service.adapter.`in`.consumer.order.dto.OrderConfirmedEvent
import br.com.souza.inventory_service.application.domain.model.ConfirmReservationCommand
import br.com.souza.inventory_service.application.ports.`in`.ConfirmReservationUseCase
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
class OrderConfirmedKafkaConsumer(
    private val confirmReservation: ConfirmReservationUseCase,
    private val traceContextExtractor: TraceContextExtractor
) {
    private val logger = LoggerFactory.getLogger(OrderConfirmedKafkaConsumer::class.java)
    private val tracer = GlobalOpenTelemetry.getTracer("inventory-service")

    @KafkaListener(topics = ["orders.confirmed"], groupId = "inventory-service", containerFactory = "ordersConfirmedKafkaListenerContainerFactory")
    fun consume(
        @Payload event: OrderConfirmedEvent,
        @Header(name = "traceparent", required = false) traceParent: String?
    ) {
        val extractedContext = traceContextExtractor.extractContext(traceParent)
        val span = tracer.spanBuilder("orders.confirmed process")
            .setParent(extractedContext)
            .setSpanKind(SpanKind.CONSUMER)
            .startSpan()

        span.makeCurrent().use {
            try {
                logger.info("Received order confirmed event", kv("order_id", event.orderId))
                val command = ConfirmReservationCommand(
                    orderId = event.orderId,
                    traceParent = traceParent
                )
                confirmReservation.execute(command)
            } finally {
                span.end()
            }
        }
    }
}
