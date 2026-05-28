package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"payments-service/internal/config"
	"payments-service/internal/consumer"
	"payments-service/internal/relay"
	"payments-service/internal/repository"
	"payments-service/internal/service"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
)

func main() {
	cfg := config.Load()

	// inicia logger
	logger, err := config.InitLogger()
	if err != nil {
		log.Fatalf("logger: %v", err)
	}
	defer logger.Sync()

	// conecta ao banco de dados
	database, err := config.MustConnectMySQL(logger, cfg.MySQL.DSN)
	if err != nil {
		log.Fatalf("mysql: %v", err)
	}
	defer database.Close()

	// inicia tracer
	tracer, err := config.InitTracer()
	if err != nil {
		log.Fatalf("tracer: %v", err)
	}
	defer tracer.Shutdown(context.Background())
	otel.SetTextMapPropagator(propagation.TraceContext{})

	// repositories
	paymentRepo := repository.NewPaymentRepository(database)
	outboxRepo := repository.NewOutboxRepository(database)

	// service
	paymentService := service.NewPaymentService(logger, database, paymentRepo, outboxRepo)

	// contexto cancelável para consumer e relay
	appCtx, cancelApp := context.WithCancel(context.Background())
	defer cancelApp()

	// kafka consumer
	kafkaInventoryReader := config.NewKafkaConsumer(cfg.Kafka.Brokers, cfg.Kafka.InventoryReservedTopic, "payments-service-group")
	defer kafkaInventoryReader.Close()

	inventoryConsumer := consumer.NewInventoryConsumer(logger, kafkaInventoryReader, paymentService)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("inventory consumer panic", zap.Any("panic", r))
			}
		}()
		inventoryConsumer.Start(appCtx)
	}()

	// kafka producers (one per topic)
	authorizedWriter := config.NewKafkaProducer(cfg.Kafka.Brokers, cfg.Kafka.PaymentAuthorizedTopic)
	defer authorizedWriter.Close()

	deniedWriter := config.NewKafkaProducer(cfg.Kafka.Brokers, cfg.Kafka.PaymentDeniedTopic)
	defer deniedWriter.Close()

	writers := map[string]*kafka.Writer{
		"payments.authorized": authorizedWriter,
		"payments.denied":     deniedWriter,
	}

	// relay
	outboxRelay := relay.NewOutboxRelay(logger, outboxRepo, writers, cfg.Outbox.BatchSize)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("outbox relay panic", zap.Any("panic", r))
			}
		}()
		outboxRelay.Start(appCtx)
	}()

	r := gin.New()
	r.Use(gin.Recovery())

	// health check
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status": "UP",
		})
	})

	srv := &http.Server{
		Addr:    cfg.Server.Port,
		Handler: r,
	}

	go func() {
		logger.Info("server starting", zap.String("addr", cfg.Server.Port))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server: %v", err)
		}
	}()

	// aguarda SIGINT ou SIGTERM
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("shutting down server...")

	// cancela consumer e relay
	cancelApp()

	// dá 10s para requests em andamento terminarem
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("server forced to shutdown", zap.Error(err))
	}

	logger.Info("server exited")
}
