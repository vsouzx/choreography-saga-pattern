package br.com.souza.inventory_service.adapter.`in`.consumer.payments

import br.com.souza.inventory_service.adapter.`in`.consumer.payments.dto.PaymentsDeniedEvent
import br.com.souza.inventory_service.application.domain.model.ReleaseStockCommand
import br.com.souza.inventory_service.application.ports.`in`.ReleaseStockUseCase
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
class PaymentsDeniedKafkaConsumer (
    private val traceContextExtractor: TraceContextExtractor,
    private val releaseStock: ReleaseStockUseCase
){
    private val logger = LoggerFactory.getLogger(PaymentsDeniedKafkaConsumer::class.java)
    private val tracer = GlobalOpenTelemetry.getTracer("inventory-service")

    @KafkaListener(topics = ["payments.denied"], groupId = "inventory-service", containerFactory = "paymentsKafkaListenerContainerFactory")
    fun consume(
        @Payload payload: PaymentsDeniedEvent,
        @Header(name = "traceparent", required = false) traceParent: String?
    ) {
        val extractedContext = traceContextExtractor.extractContext(traceParent)

        val span = tracer.spanBuilder("payments.denied process")
            .setParent(extractedContext)
            .setSpanKind(SpanKind.CONSUMER)
            .startSpan()

        span.makeCurrent().use {
            try {
                logger.info("Received payment denied event", kv("order_id", payload.orderId), kv("reason", payload.reason))

                val command = ReleaseStockCommand(
                    payload.orderId,
                    payload.reason,
                    traceParent
                )

                releaseStock.execute(command)
            }finally {
                span.end()
            }
        }
    }
}