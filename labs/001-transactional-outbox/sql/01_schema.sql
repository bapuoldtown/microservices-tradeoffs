CREATE TABLE orders (id UUID PRIMARY KEY, customer_id TEXT NOT NULL, amount_cents BIGINT NOT NULL CHECK(amount_cents>0), status TEXT NOT NULL DEFAULT 'CREATED', created_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE outbox_events (event_id UUID PRIMARY KEY, aggregate_id UUID NOT NULL REFERENCES orders(id), event_type TEXT NOT NULL, payload JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), published_at TIMESTAMPTZ);
CREATE TABLE inventory (product_id TEXT PRIMARY KEY, available_quantity INT NOT NULL CHECK(available_quantity>=0), reserved_quantity INT NOT NULL DEFAULT 0 CHECK(reserved_quantity>=0));
CREATE TABLE processed_events (event_id UUID PRIMARY KEY, processed_at TIMESTAMPTZ NOT NULL DEFAULT now());
