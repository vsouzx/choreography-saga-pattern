package main

import (
	"context"
	"log"
	"net/http"
	"orders-service/internal/config"
	"orders-service/internal/consumer"
	"orders-service/internal/handler"
	"orders-service/internal/middleware"
	"orders-service/internal/relay"
	"orders-service/internal/repository"
	"orders-service/internal/service"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/contrib/bridges/otelzap"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	// carrega as configs
	cfg := config.Load()

	// inicia logger
	baseLogger, err := config.InitLogger()
	if err != nil {
		log.Fatalf("logger: %v", err)
	}
	defer baseLogger.Sync()

	// inicia log provider (OTLP)
	logProvider, err := config.InitLoggerProvider(cfg.Otel, "orders-service")
	if err != nil {
		log.Fatalf("log provider: %v", err)
	}
	defer logProvider.Shutdown(context.Background())

	// bridge zap -> otel
	otelCore := otelzap.NewCore("orders-service", otelzap.WithLoggerProvider(logProvider))
	logger := zap.New(zapcore.NewTee(baseLogger.Core(), otelCore))
	defer logger.Sync()

	// inicia tracer
	tracer, err := config.InitTracer(cfg.Otel)
	if err != nil {
		log.Fatalf("tracer: %v", err)
	}
	defer tracer.Shutdown(context.Background())
	otel.SetTextMapPropagator(propagation.TraceContext{})

	// conecta ao banco de dados
	database, err := config.MustConnectMySQL(logger, cfg.MySQL.DSN)
	if err != nil {
		log.Fatalf("mysql: %v", err)
	}
	defer database.Close()

	// conecta ao redis
	redis, err := config.MustConnectRedis(logger, cfg.Redis.Addr, cfg.Redis.DB)
	if err != nil {
		log.Fatalf("redis: %v", err)
	}
	defer redis.Close()

	// kafka producers
	ordersCreatedWriter := config.NewKafkaProducer(cfg.Kafka.Brokers, cfg.Kafka.OrdersCreatedTopic)
	defer ordersCreatedWriter.Close()

	ordersConfirmedWriter := config.NewKafkaProducer(cfg.Kafka.Brokers, cfg.Kafka.OrdersConfirmedTopic)
	defer ordersConfirmedWriter.Close()

	writers := map[string]*kafka.Writer{
		cfg.Kafka.OrdersCreatedTopic:   ordersCreatedWriter,
		cfg.Kafka.OrdersConfirmedTopic: ordersConfirmedWriter,
	}

	// repositories
	ordersRepo := repository.NewOrderRepository()
	outboxRepo := repository.NewOutboxRepository(database)

	relayCtx, cancelRelay := context.WithCancel(context.Background())
	defer cancelRelay()

	// relay
	outboxRelay := relay.NewOutboxRelay(logger, outboxRepo, writers, cfg.Outbox.BatchSize)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("outbox relay panic", zap.Any("panic", r))
			}
		}()
		outboxRelay.Start(relayCtx)
	}()

	// kafka consumer
	kafkaInventoryReader := config.NewKafkaConsumer(cfg.Kafka.Brokers, cfg.Kafka.InventoryTopic, "orders-service-group")
	defer kafkaInventoryReader.Close()

	kafkaInventoryReleasedReader := config.NewKafkaConsumer(cfg.Kafka.Brokers, cfg.Kafka.InventoryReleasedTopic, "orders-service-group")
	defer kafkaInventoryReleasedReader.Close()

	kafkaPaymentsReader := config.NewKafkaConsumer(cfg.Kafka.Brokers, cfg.Kafka.PaymentsTopic, "orders-service-group")
	defer kafkaPaymentsReader.Close()

	// services
	orderService := service.NewOrderService(logger, database, ordersRepo, outboxRepo)

	// inventory consumer
	inventoryConsumer := consumer.NewInventoryConsumer(logger, kafkaInventoryReader, orderService)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("inventory consumer panic", zap.Any("panic", r))
			}
		}()
		inventoryConsumer.Start(relayCtx)
	}()

	// inventory released consumer
	inventoryReleasedConsumer := consumer.NewInventoryReleasedConsumer(logger, kafkaInventoryReleasedReader, orderService)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("inventory released consumer panic", zap.Any("panic", r))
			}
		}()
		inventoryReleasedConsumer.Start(relayCtx)
	}()

	// payments consumer
	paymentsConsumer := consumer.NewPaymentsConsumer(logger, kafkaPaymentsReader, orderService)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("payments consumer panic", zap.Any("panic", r))
			}
		}()
		paymentsConsumer.Start(relayCtx)
	}()

	// handlers
	orderHandler := handler.NewOrderHandler(logger, orderService)

	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(otelgin.Middleware("orders-service"))
	r.Use(middleware.ErrorHandler(logger))

	// health check
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status": "UP",
		})
	})

	// routes
	v1 := r.Group("/v1")
	{
		v1.GET("/orders", orderHandler.List)
		v1.POST("/orders",
			middleware.IdempotencyMiddleware(logger, redis),
			orderHandler.Create)
	}

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

	// dá 10s para requests em andamento terminarem
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("server forced to shutdown", zap.Error(err))
	}

	logger.Info("server exited")
	// os defers (database.Close, redis.Close, kafkaProducer.Close, logger.Sync) executam aqui
}
