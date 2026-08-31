package documents

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Document struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	Body  string `json:"body"`
}

type Store struct {
	db *pgxpool.Pool
}

func NewStore(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

func (s *Store) Upsert(ctx context.Context, d Document) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO documents (id, title, body, updated_at)
		 VALUES ($1, $2, $3, now())
		 ON CONFLICT (id) DO UPDATE
		 SET title = EXCLUDED.title, body = EXCLUDED.body, updated_at = now()`,
		d.ID, d.Title, d.Body)
	return err
}

func (s *Store) NeedingIndex(ctx context.Context) ([]string, error) {
	rows, err := s.db.Query(ctx,
		`SELECT id FROM documents
		 WHERE indexed_at IS NULL OR indexed_at < updated_at
		 ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func (s *Store) Index(ctx context.Context, id string) error {
	_, err := s.db.Exec(ctx,
		`WITH source AS (
		   SELECT id, to_tsvector('english', title || ' ' || body) AS terms, updated_at
		   FROM documents WHERE id = $1
		 ), written AS (
		   INSERT INTO document_index (document_id, terms, indexed_at)
		   SELECT id, terms, now() FROM source
		   ON CONFLICT (document_id) DO UPDATE
		   SET terms = EXCLUDED.terms, indexed_at = now()
		   RETURNING document_id
		 )
		 UPDATE documents SET indexed_at = now()
		 FROM source
		 WHERE documents.id = source.id AND documents.updated_at = source.updated_at`,
		id)
	return err
}

func (s *Store) Search(ctx context.Context, query string) ([]Document, error) {
	rows, err := s.db.Query(ctx,
		`SELECT d.id, d.title, d.body
		 FROM document_index i JOIN documents d ON d.id = i.document_id
		 WHERE i.terms @@ plainto_tsquery('english', $1)
		 ORDER BY d.id`,
		query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToStructByPos[Document])
}
