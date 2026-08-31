package webhooks

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	StatusQueued    = "queued"
	StatusDelivered = "delivered"
	StatusFailed    = "failed"
)

type Subscriber struct {
	ID  string
	URL string
}

type Store struct {
	db *pgxpool.Pool
}

func NewStore(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

func (s *Store) ActiveSubscribers(ctx context.Context) ([]Subscriber, error) {
	rows, err := s.db.Query(ctx,
		`SELECT id, url FROM subscribers WHERE active ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToStructByPos[Subscriber])
}

func (s *Store) Queue(ctx context.Context, d Delivery) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO deliveries (id, event_id, subscriber_id, status)
		 VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING`,
		d.ID, d.EventID, d.SubscriberID, StatusQueued)
	return err
}

func (s *Store) Record(ctx context.Context, id, status string, attempt int) error {
	_, err := s.db.Exec(ctx,
		`UPDATE deliveries SET status = $2, attempts = $3, updated_at = now() WHERE id = $1`,
		id, status, attempt)
	return err
}
