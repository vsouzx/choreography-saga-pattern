package domain

import "time"

type PaymentTypeEnum string
type StatusEnum string

const (
	Pix        PaymentTypeEnum = "PIX"
	CreditCard PaymentTypeEnum = "CREDIT_CARD"
	Boleto     PaymentTypeEnum = "BOLETO"

	Pending   StatusEnum = "PENDING"
	Completed StatusEnum = "CONFIRMED"
	Canceled  StatusEnum = "CANCELED"
)

type OrderRequest struct {
	IdempotencyKey string          `json:"-"` // vem no header
	UserID         string          `json:"userId" binding:"required,uuid"`
	ProductID      int             `json:"productId" binding:"required"`
	Quantity       int             `json:"quantity" binding:"required,gte=1"`
	PaymentType    PaymentTypeEnum `json:"paymentType" binding:"required,oneof=PIX CREDIT_CARD BOLETO"`
}

type OrderEvent struct {
	OrderID     string          `json:"orderId"`
	UserID      string          `json:"userId"`
	ProductID   int             `json:"productId"`
	Quantity    int             `json:"quantity"`
	PaymentType PaymentTypeEnum `json:"paymentType"`
	CreatedAt   time.Time       `json:"createdAt"`
}

type InsufficientStockEvent struct {
	OrderID   string    `json:"orderId"`
	Reason    string    `json:"reason"`
	CreatedAt time.Time `json:"createdAt"`
}

type InventoryReleasedEvent struct {
	OrderID string `json:"orderId"`
	Status  string `json:"status"`
	Reason  string `json:"reason"`
}

type OrderConfirmedEvent struct {
	OrderID   string    `json:"orderId"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

type OrderResponse struct {
	ID          string          `json:"id"`
	UserID      string          `json:"userId"`
	ProductID   int             `json:"productId"`
	Quantity    int             `json:"quantity"`
	PaymentType PaymentTypeEnum `json:"paymentType"`
	Status      StatusEnum      `json:"status"`
	Reason      *string         `json:"reason,omitempty"`
	CreatedAt   time.Time       `json:"createdAt"`
}
