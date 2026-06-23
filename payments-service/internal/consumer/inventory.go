package consumer

import (
	"context"
	"encoding/json"
	"payments-service/internal/domain"
	"payments-service/internal/logger"
	"payments-service/internal/service"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
)

type InventoryConsumer struct {
	logger         *zap.Logger
	reader         *kafka.Reader
	paymentService *service.PaymentService
}

func NewInventoryConsumer(logger *zap.Logger, reader *kafka.Reader, paymentService *service.PaymentService) *InventoryConsumer {
	return &InventoryConsumer{
		logger:         logger,
		reader:         reader,
		paymentService: paymentService,
	}
}

func (c *InventoryConsumer) Start(ctx context.Context) {
	c.logger.Info("inventory consumer started", zap.String("topic", c.reader.Config().Topic))

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.logger.Info("inventory consumer stopped")
				return
			}
			c.logger.Error("error fetching message", zap.Error(err))
			continue
		}

		c.processMessage(ctx, msg)
	}
}

func (c *InventoryConsumer) processMessage(ctx context.Context, msg kafka.Message) {
	traceParent := extractTraceParent(msg.Headers)
	ctx = extractTraceContext(ctx, msg.Headers)

	tracer := otel.Tracer("inventory-consumer")
	ctx, span := tracer.Start(ctx, "inventory.reserved.process")
	defer span.End()

	log := logger.FromContext(ctx, c.logger)

	log.Info("received inventory.reserved message", zap.String("key", string(msg.Key)))

	var event domain.InventoryReservedEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Error("error unmarshalling message",
			zap.Error(err),
			zap.String("raw_value", string(msg.Value)),
		)
		c.commitMessage(ctx, msg, log)
		return
	}

	log.Info("processing inventory.reserved event",
		zap.String("order_id", event.OrderID),
		zap.Int("amount", event.Amount),
		zap.String("payment_type", event.PaymentType),
	)

	if err := c.paymentService.ProcessPayment(ctx, event, traceParent); err != nil {
		log.Error("error processing payment",
			zap.String("order_id", event.OrderID),
			zap.Error(err),
		)
		// don't commit — message will be redelivered
		return
	}

	c.commitMessage(ctx, msg, log)
}

func (c *InventoryConsumer) commitMessage(ctx context.Context, msg kafka.Message, log *zap.Logger) {
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		log.Error("error committing message", zap.Error(err))
	}
}

func extractTraceParent(headers []kafka.Header) string {
	for _, h := range headers {
		if h.Key == "traceparent" {
			return string(h.Value)
		}
	}
	return ""
}

func extractTraceContext(ctx context.Context, headers []kafka.Header) context.Context {
	carrier := make(propagation.MapCarrier)
	for _, h := range headers {
		carrier.Set(h.Key, string(h.Value))
	}
	propagator := otel.GetTextMapPropagator()
	return propagator.Extract(ctx, carrier)
}
