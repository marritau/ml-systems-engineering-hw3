CREATE TABLE IF NOT EXISTS transaction_scores (
    transaction_id TEXT PRIMARY KEY,
    score DOUBLE PRECISION NOT NULL,
    fraud_flag SMALLINT NOT NULL,
    model_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT transaction_scores_score_range CHECK (score >= 0 AND score <= 1),
    CONSTRAINT transaction_scores_fraud_flag_binary CHECK (fraud_flag IN (0, 1))
);

CREATE INDEX IF NOT EXISTS idx_transaction_scores_created_at
    ON transaction_scores (created_at DESC);

CREATE TABLE IF NOT EXISTS labeled_transactions (
    record_id TEXT PRIMARY KEY,
    transaction_time TIMESTAMP NOT NULL,
    merch TEXT,
    cat_id TEXT,
    amount DOUBLE PRECISION,
    name_1 TEXT,
    name_2 TEXT,
    gender TEXT,
    street TEXT,
    one_city TEXT,
    us_state TEXT,
    post_code TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    population_city INTEGER,
    jobs TEXT,
    merchant_lat DOUBLE PRECISION,
    merchant_lon DOUBLE PRECISION,
    target SMALLINT NOT NULL,
    batch_month DATE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_labeled_transactions_time
    ON labeled_transactions (transaction_time);

CREATE TABLE IF NOT EXISTS pipeline_state (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    training_window_start DATE NOT NULL,
    training_window_end DATE NOT NULL,
    next_training_month DATE NOT NULL,
    next_supply_month DATE NOT NULL,
    published_model_version TEXT,
    published_roc_auc DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

