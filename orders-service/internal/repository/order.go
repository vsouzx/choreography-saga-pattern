package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"orders-service/internal/domain"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
)

type OrdersRepositoryInterface interface {
	Save(ctx context.Context, tx DBTX, request domain.OrderRequest) (string, error)
	UpdateStatusToCanceled(ctx context.Context, tx DBTX, orderId string, reason string) error
	ConfirmOrder(ctx context.Context, tx DBTX, orderId string) (bool, error)
	FindAll(ctx context.Context, db *sql.DB) ([]domain.OrderResponse, error)
}

type OrderRepository struct {
}

func NewOrderRepository() *OrderRepository {
	return &OrderRepository{}
}

func (r *OrderRepository) Save(ctx context.Context, tx DBTX, request domain.OrderRequest) (string, error) {
	orderID := uuid.New().String()

	query := `INSERT INTO orders (id, idempotency_key, user_id, product_id, quantity, payment_type, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, NOW())`

	_, err := tx.ExecContext(ctx, query, orderID, request.IdempotencyKey, request.UserID, request.ProductID, request.Quantity, request.PaymentType, domain.Pending)
	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return "", domain.NewConflictError("duplicate idempotency key: this idempotency key was already used")
		}
		return "", fmt.Errorf("saving order: %w", err)
	}

	return orderID, nil
}

func (r *OrderRepository) UpdateStatusToCanceled(ctx context.Context, tx DBTX, orderId string, reason string) error {
	query := `UPDATE orders SET status = ?, reason = ? WHERE id = ? AND status = ?`

	_, err := tx.ExecContext(ctx, query, domain.Canceled, reason, orderId, domain.Pending)
	if err != nil {
		return fmt.Errorf("updating order status to canceled: %w", err)
	}

	return nil
}

func (r *OrderRepository) ConfirmOrder(ctx context.Context, tx DBTX, orderId string) (bool, error) {
	query := `UPDATE orders SET status = ?, updated_at = NOW() WHERE id = ? AND status = ?`

	result, err := tx.ExecContext(ctx, query, domain.Completed, orderId, domain.Pending)
	if err != nil {
		return false, fmt.Errorf("updating order status: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("getting rows affected: %w", err)
	}

	return rowsAffected > 0, nil
}

func (r *OrderRepository) FindAll(ctx context.Context, db *sql.DB) ([]domain.OrderResponse, error) {
	query := `SELECT id, user_id, product_id, quantity, payment_type, status, reason, created_at FROM orders ORDER BY created_at DESC`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("querying orders: %w", err)
	}
	defer rows.Close()

	var orders []domain.OrderResponse
	for rows.Next() {
		var o domain.OrderResponse
		if err := rows.Scan(&o.ID, &o.UserID, &o.ProductID, &o.Quantity, &o.PaymentType, &o.Status, &o.Reason, &o.CreatedAt); err != nil {
			return nil, fmt.Errorf("scanning order: %w", err)
		}
		orders = append(orders, o)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating orders: %w", err)
	}

	return orders, nil
}
