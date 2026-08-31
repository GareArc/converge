CREATE TABLE orders (
  id        text PRIMARY KEY,
  status    text        NOT NULL,
  placed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE subscribers (
  id     text PRIMARY KEY,
  url    text    NOT NULL,
  active boolean NOT NULL DEFAULT true
);

CREATE TABLE deliveries (
  id            text PRIMARY KEY,
  event_id      text        NOT NULL,
  subscriber_id text        NOT NULL REFERENCES subscribers (id),
  status        text        NOT NULL,
  attempts      integer     NOT NULL DEFAULT 0,
  updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE documents (
  id         text PRIMARY KEY,
  title      text        NOT NULL,
  body       text        NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  indexed_at timestamptz
);

CREATE TABLE document_index (
  document_id text PRIMARY KEY REFERENCES documents (id) ON DELETE CASCADE,
  terms       tsvector    NOT NULL,
  indexed_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX document_index_terms ON document_index USING gin (terms);

INSERT INTO subscribers (id, url, active) VALUES
  ('sub-slow', 'http://httpbin:80/status/429', true),
  ('sub-ok',   'http://httpbin:80/status/200', true),
  ('sub-gone', 'http://httpbin:80/status/410', true);
