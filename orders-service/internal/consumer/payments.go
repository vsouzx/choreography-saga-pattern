package consumer

import (
	"context"
	"encoding/json"
	"orders-service/internal/domain"
	"orders-service/internal/logger"
	"orders-service/internal/service"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

type PaymentsConsumer struct {
	logger       *zap.Logger
	reader       *kafka.Reader
	orderService *service.OrderService
}

func NewPaymentsConsumer(logger *zap.Logger, reader *kafka.Reader, orderService *service.OrderService) *PaymentsConsumer {
	return &PaymentsConsumer{
		logger:       logger,
		reader:       reader,
		orderService: orderService,
	}
}

func (c *PaymentsConsumer) Start(ctx context.Context) {
	c.logger.Info("payments consumer started", zap.String("topic", c.reader.Config().Topic))

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.logger.Info("payments consumer stopped")
				return
			}
			c.logger.Error("error fetching message", zap.Error(err))
			continue
		}

		c.processMessage(ctx, msg)
	}
}

func (c *PaymentsConsumer) processMessage(ctx context.Context, msg kafka.Message) {
	ctx = extractTraceContext(ctx, msg.Headers)

	tracer := otel.Tracer("payments-consumer")
	ctx, span := tracer.Start(ctx, "payments.authorized.process")
	defer span.End()

	log := logger.FromContext(ctx, c.logger)

	log.Info("received payments.authorized message", zap.String("key", string(msg.Key)))

	var event domain.PaymentsAuthorizedEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Error("error unmarshalling message",
			zap.Error(err),
			zap.String("raw_value", string(msg.Value)),
		)
		// commit to skip malformed message — retrying won't help
		c.commitMessage(ctx, msg, log)
		return
	}

	log.Info("processing payments.authorized event",
		zap.String("order_id", event.OrderID),
		zap.String("status", event.Status),
	)

	if err := c.orderService.ConfirmOrder(ctx, event.OrderID); err != nil {
		log.Error("error confirming order",
			zap.String("order_id", event.OrderID),
			zap.Error(err),
		)
		// don't commit — message will be redelivered
		return
	}

	c.commitMessage(ctx, msg, log)
}

func (c *PaymentsConsumer) commitMessage(ctx context.Context, msg kafka.Message, log *zap.Logger) {
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		log.Error("error committing message", zap.Error(err))
	}
}
