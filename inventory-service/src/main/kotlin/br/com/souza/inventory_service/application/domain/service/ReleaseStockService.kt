package br.com.souza.inventory_service.application.domain.service

import br.com.souza.inventory_service.application.domain.model.OutboxEvent
import br.com.souza.inventory_service.application.domain.model.ReleaseStockCommand
import br.com.souza.inventory_service.application.domain.model.ReservationStatus
import br.com.souza.inventory_service.application.ports.`in`.ReleaseStockUseCase
import br.com.souza.inventory_service.application.ports.out.OutboxEventRepositoryPort
import br.com.souza.inventory_service.application.ports.out.StockRepositoryPort
import br.com.souza.inventory_service.application.ports.out.StockReservationRepositoryPort
import com.fasterxml.jackson.databind.ObjectMapper
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime
import java.util.UUID

@Service
class ReleaseStockService(
    private val reservationRepository: StockReservationRepositoryPort,
    private val stockRepository: StockRepositoryPort,
    private val outboxRepository: OutboxEventRepositoryPort
) : ReleaseStockUseCase {
    private val logger = LoggerFactory.getLogger(ReleaseStockService::class.java)
    private val objectMapper = ObjectMapper()
    private val aggregateType = "ORDER"
    private val eventType = "INVENTORY_RELEASED"

    @Transactional
    override fun execute(command: ReleaseStockCommand){
        val aggregateId = command.orderId

        if (outboxRepository.existsByAggregateIdAndAggregateType(aggregateId, aggregateType, listOf(eventType))) {
            logger.info("Event already processed, skipping", kv("order_id", command.orderId), kv("event_type", eventType))
            return
        }

        logger.info("Processing stock release", kv("order_id", command.orderId))

        val reservation = reservationRepository.findByOrderId(command.orderId)
        if (reservation == null) {
            logger.warn("Reservation not found", kv("order_id", command.orderId))
            return
        }

        val stock = stockRepository.findByProductIdWithLock(reservation.productId)
        if (stock == null) {
            logger.warn("Stock not found", kv("product_id", reservation.productId), kv("order_id", command.orderId))
            return
        }

        val updatedStock = stock.copy(
            quantityAvailable = stock.quantityAvailable + reservation.quantity,
            updatedAt = LocalDateTime.now()
        )
        stockRepository.save(updatedStock)
        logger.info("Stock restored", kv("product_id", reservation.productId), kv("previous_qty", stock.quantityAvailable), kv("new_qty", updatedStock.quantityAvailable))

        val updatedReservation = reservation.copy(status = ReservationStatus.RELEASED)
        reservationRepository.save(updatedReservation)
        logger.info("Reservation status updated to RELEASED", kv("order_id", command.orderId))

        val payload = objectMapper.writeValueAsString(
            mapOf(
                "orderId" to command.orderId,
                "status" to ReservationStatus.RELEASED.name,
                "reason" to command.reason
            )
        )

        val outboxEvent = OutboxEvent(
            id = UUID.randomUUID().toString(),
            aggregateId = command.orderId,
            aggregateType = aggregateType,
            eventType = eventType,
            topic = "inventory.released",
            payload = payload,
            traceParent = command.traceParent
        )
        outboxRepository.save(outboxEvent)
        logger.info("Outbox event saved", kv("topic", "inventory.released"), kv("order_id", command.orderId))
    }
}