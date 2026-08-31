package orders

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	StatusPending   = "pending"
	StatusPaid      = "paid"
	StatusCancelled = "cancelled"
)

type Order struct {
	ID       string    `json:"id"`
	Status   string    `json:"status"`
	PlacedAt time.Time `json:"placed_at"`
}

type Store struct {
	db *pgxpool.Pool
}

func NewStore(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

func (s *Store) Create(ctx context.Context, id string) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO orders (id, status) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING`,
		id, StatusPending)
	return err
}

func (s *Store) Pay(ctx context.Context, id string) error {
	_, err := s.db.Exec(ctx,
		`UPDATE orders SET status = $2 WHERE id = $1 AND status = $3`,
		id, StatusPaid, StatusPending)
	return err
}

func (s *Store) Get(ctx context.Context, id string) (Order, bool, error) {
	var o Order
	err := s.db.QueryRow(ctx,
		`SELECT id, status, placed_at FROM orders WHERE id = $1`, id).
		Scan(&o.ID, &o.Status, &o.PlacedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Order{}, false, nil
	}
	if err != nil {
		return Order{}, false, err
	}
	return o, true, nil
}

func (s *Store) PendingOlderThan(age time.Duration) func(context.Context) ([]string, error) {
	return func(ctx context.Context) ([]string, error) {
		rows, err := s.db.Query(ctx,
			`SELECT id FROM orders
			 WHERE status = $1 AND placed_at < now() - $2::interval
			 ORDER BY id`,
			StatusPending, age.String())
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		return pgx.CollectRows(rows, pgx.RowTo[string])
	}
}

func (s *Store) CancelIfUnpaid(ctx context.Context, id string, age time.Duration) error {
	_, err := s.db.Exec(ctx,
		`UPDATE orders SET status = $2
		 WHERE id = $1 AND status = $3 AND placed_at < now() - $4::interval`,
		id, StatusCancelled, StatusPending, age.String())
	return err
}
