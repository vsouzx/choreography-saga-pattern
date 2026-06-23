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

type InventoryReleasedConsumer struct {
	logger       *zap.Logger
	reader       *kafka.Reader
	orderService *service.OrderService
}

func NewInventoryReleasedConsumer(logger *zap.Logger, reader *kafka.Reader, orderService *service.OrderService) *InventoryReleasedConsumer {
	return &InventoryReleasedConsumer{
		logger:       logger,
		reader:       reader,
		orderService: orderService,
	}
}

func (c *InventoryReleasedConsumer) Start(ctx context.Context) {
	c.logger.Info("inventory released consumer started", zap.String("topic", c.reader.Config().Topic))

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.logger.Info("inventory released consumer stopped")
				return
			}
			c.logger.Error("error fetching message", zap.Error(err))
			continue
		}

		c.processMessage(ctx, msg)
	}
}

func (c *InventoryReleasedConsumer) processMessage(ctx context.Context, msg kafka.Message) {
	ctx = extractTraceContext(ctx, msg.Headers)

	tracer := otel.Tracer("inventory-released-consumer")
	ctx, span := tracer.Start(ctx, "inventory.released.process")
	defer span.End()

	log := logger.FromContext(ctx, c.logger)

	log.Info("received inventory.released message", zap.String("key", string(msg.Key)))

	var event domain.InventoryReleasedEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Error("error unmarshalling message",
			zap.Error(err),
			zap.String("raw_value", string(msg.Value)),
		)
		// commit to skip malformed message — retrying won't help
		c.commitMessage(ctx, msg, log)
		return
	}

	log.Info("processing inventory released event",
		zap.String("order_id", event.OrderID),
		zap.String("reason", event.Reason),
	)

	if err := c.orderService.CancelOrder(ctx, event.OrderID, event.Reason); err != nil {
		log.Error("error canceling order",
			zap.String("order_id", event.OrderID),
			zap.Error(err),
		)
		// don't commit — message will be redelivered
		return
	}

	c.commitMessage(ctx, msg, log)
}

func (c *InventoryReleasedConsumer) commitMessage(ctx context.Context, msg kafka.Message, log *zap.Logger) {
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		log.Error("error committing message", zap.Error(err))
	}
}
