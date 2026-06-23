package relay

import (
	"context"
	"fmt"
	"orders-service/internal/logger"
	"orders-service/internal/repository"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type OutboxRelay struct {
	logger           *zap.Logger
	outboxRepository repository.OutboxRepositoryInterface
	writers          map[string]*kafka.Writer
	interval         time.Duration
	batchSize        int
}

func NewOutboxRelay(logger *zap.Logger,
	outboxRepository repository.OutboxRepositoryInterface,
	writers map[string]*kafka.Writer,
	batchSize int) *OutboxRelay {
	return &OutboxRelay{
		logger:           logger,
		outboxRepository: outboxRepository,
		writers:          writers,
		interval:         5 * time.Second,
		batchSize:        batchSize,
	}
}

func (or *OutboxRelay) Start(ctx context.Context) {
	ticker := time.NewTicker(or.interval)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			or.poll(ctx)
		}
	}
}

func (or *OutboxRelay) poll(ctx context.Context) {
	log := logger.FromContext(ctx, or.logger)

	outboxEvents, err := or.outboxRepository.FindAndLockPendingEvents(ctx, or.batchSize)
	if err != nil {
		log.Error("error on find pending events", zap.Error(err))
		return
	}

	for _, event := range outboxEvents {
		log.Info("sending event", zap.Any("event_id", event.ID), zap.Int("retries_count", event.RetriesCount))

		writer, ok := or.writers[event.EventType]
		if !ok {
			log.Error("no writer configured for event type", zap.String("event_type", event.EventType))
			if err = or.outboxRepository.MarkEventAsFailed(ctx, event.ID); err != nil {
				log.Error("error on mark event as failed", zap.Error(err))
			}
			continue
		}

		message := kafka.Message{
			Key:   []byte(fmt.Sprintf("%s-%s", event.AggregateType, event.AggregateId)),
			Value: event.Payload,
			Headers: []kafka.Header{
				{Key: "traceparent", Value: []byte(event.TraceParent)},
			},
		}

		if err := writer.WriteMessages(ctx, message); err != nil {
			log.Error("error on write outbox event", zap.Any("outbox_event_id", event.ID), zap.Error(err))
			if err = or.outboxRepository.MarkEventAsFailed(ctx, event.ID); err != nil {
				log.Error("error on mark event as failed", zap.Error(err))
			}
			continue
		}

		if err := or.outboxRepository.MarkEventAsSent(ctx, event.ID); err != nil {
			log.Error("error on mark event as sent", zap.Error(err))
		} else {
			log.Info("event sent", zap.String("outbox_event_id", event.ID))
		}
	}
}
