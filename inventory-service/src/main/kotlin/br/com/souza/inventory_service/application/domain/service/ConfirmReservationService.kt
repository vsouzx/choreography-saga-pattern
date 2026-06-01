package br.com.souza.inventory_service.application.domain.service

import br.com.souza.inventory_service.application.domain.model.ConfirmReservationCommand
import br.com.souza.inventory_service.application.domain.model.ReservationStatus
import br.com.souza.inventory_service.application.ports.`in`.ConfirmReservationUseCase
import br.com.souza.inventory_service.application.ports.out.StockReservationRepositoryPort
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class ConfirmReservationService(
    private val reservationRepository: StockReservationRepositoryPort
) : ConfirmReservationUseCase {

    private val logger = LoggerFactory.getLogger(ConfirmReservationService::class.java)

    @Transactional
    override fun execute(command: ConfirmReservationCommand) {
        logger.info("Processing reservation confirmation", kv("order_id", command.orderId))

        val reservation = reservationRepository.findByOrderId(command.orderId)
        if (reservation == null) {
            logger.warn("Reservation not found, skipping confirmation", kv("order_id", command.orderId))
            return
        }

        val updatedReservation = reservation.copy(status = ReservationStatus.CONFIRMED)
        reservationRepository.save(updatedReservation)
        logger.info("Reservation confirmed", kv("order_id", command.orderId), kv("reservation_id", reservation.id))
    }
}
