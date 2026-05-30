package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"orders-service/internal/domain"
	"orders-service/internal/logger"
	"orders-service/internal/repository"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type OrderService struct {
	logger           *zap.Logger
	db               *sql.DB
	orderRepository  repository.OrdersRepositoryInterface
	outboxRepository repository.OutboxRepositoryInterface
}

func NewOrderService(logger *zap.Logger, db *sql.DB,
	orderRepository repository.OrdersRepositoryInterface,
	outboxRepository repository.OutboxRepositoryInterface) *OrderService {
	return &OrderService{
		logger:           logger,
		db:               db,
		orderRepository:  orderRepository,
		outboxRepository: outboxRepository,
	}
}

func (os *OrderService) CreateOrder(ctx context.Context, request domain.OrderRequest) error {
	log := logger.FromContext(ctx, os.logger)

	tx, err := os.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback() // garante rollback se não chegar no Commit

	orderId, err := os.orderRepository.Save(ctx, tx, request)
	if err != nil {
		return fmt.Errorf("creating order: %w", err)
	}

	orderEvent := domain.OrderEvent{
		OrderID:     orderId,
		UserID:      request.UserID,
		ProductID:   request.ProductID,
		Quantity:    request.Quantity,
		PaymentType: request.PaymentType,
		CreatedAt:   time.Now(),
	}

	payload, err := json.Marshal(orderEvent)
	if err != nil {
		return fmt.Errorf("marshalling order event: %w", err)
	}

	outboxModel := domain.OutboxModel{
		AggregateType: "ORDER",
		AggregateId:   orderId,
		EventType:     "orders.created",
		Payload:       payload,
		TraceParent:   traceParentFromContext(ctx),
		Status:        "PENDING",
		RetriesCount:  0,
		MaxRetries:    3,
		CreatedAt:     time.Now(),
		SentAt:        nil,
	}

	if err := os.outboxRepository.Save(ctx, tx, outboxModel); err != nil {
		return fmt.Errorf("creating order: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	log = log.With(zap.String("idempotencyKey", request.IdempotencyKey))

	log.Info("transaction committed")
	log.With(zap.String("order_id", orderId)).Info("Order created")

	return nil
}

func (os *OrderService) CancelOrder(ctx context.Context, orderId string, reason string) error {
	log := logger.FromContext(ctx, os.logger)

	if err := os.orderRepository.UpdateStatusToCanceled(ctx, os.db, orderId, reason); err != nil {
		log.Error("failed to cancel order", zap.String("order_id", orderId), zap.Error(err))
		return fmt.Errorf("canceling order: %w", err)
	}

	log.Info("order canceled", zap.String("order_id", orderId), zap.String("reason", reason))
	return nil
}

func (os *OrderService) ConfirmOrder(ctx context.Context, orderId string) error {
	log := logger.FromContext(ctx, os.logger)

	tx, err := os.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()

	updated, err := os.orderRepository.ConfirmOrder(ctx, tx, orderId)
	if err != nil {
		return fmt.Errorf("confirming order: %w", err)
	}

	if !updated {
		log.Info("order already processed, skipping", zap.String("order_id", orderId))
		return nil
	}

	event := domain.OrderConfirmedEvent{
		OrderID:   orderId,
		Status:    string(domain.Completed),
		Timestamp: time.Now(),
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshalling order confirmed event: %w", err)
	}

	outboxModel := domain.OutboxModel{
		AggregateType: "ORDER",
		AggregateId:   orderId,
		EventType:     "orders.confirmed",
		Payload:       payload,
		TraceParent:   traceParentFromContext(ctx),
		Status:        "PENDING",
		RetriesCount:  0,
		MaxRetries:    3,
		CreatedAt:     time.Now(),
		SentAt:        nil,
	}

	if err := os.outboxRepository.Save(ctx, tx, outboxModel); err != nil {
		return fmt.Errorf("saving outbox event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	log.Info("order confirmed", zap.String("order_id", orderId))
	return nil
}

func traceParentFromContext(ctx context.Context) string {
	span := trace.SpanFromContext(ctx)
	sc := span.SpanContext()

	if !sc.IsValid() {
		return ""
	}

	return fmt.Sprintf("00-%s-%s-%s",
		sc.TraceID().String(),
		sc.SpanID().String(),
		sc.TraceFlags().String(),
	)
}
