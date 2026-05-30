package handler

import (
	"net/http"
	"orders-service/internal/domain"
	"orders-service/internal/logger"
	"orders-service/internal/service"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type OrderHandler struct {
	logger       *zap.Logger
	orderService *service.OrderService
}

func NewOrderHandler(logger *zap.Logger,
	orderService *service.OrderService) *OrderHandler {
	return &OrderHandler{
		logger:       logger,
		orderService: orderService,
	}
}

func (oh *OrderHandler) Create(c *gin.Context) {
	log := logger.FromContext(c.Request.Context(), oh.logger)

	log.With(zap.String("idempotencyKey", c.GetString("idempotencyKey"))).Info("received order request")
	var orderRequest domain.OrderRequest
	if err := c.ShouldBindJSON(&orderRequest); err != nil {
		log.With(zap.String("idempotencyKey", c.GetString("idempotencyKey"))).Info("invalid request body")
		_ = c.Error(domain.NewBadRequestError("invalid request body"))
		return
	}

	orderRequest.IdempotencyKey = c.GetString("idempotencyKey")

	if err := oh.orderService.CreateOrder(c.Request.Context(), orderRequest); err != nil {
		_ = c.Error(err)
		return
	}
	c.Status(http.StatusAccepted)
}
