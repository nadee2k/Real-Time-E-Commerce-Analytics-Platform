CREATE TABLE IF NOT EXISTS dim_user (
    user_key SERIAL PRIMARY KEY,
    user_id INT UNIQUE NOT NULL,
    country TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id INT UNIQUE NOT NULL,
    category TEXT,
    unit_price NUMERIC(10,2),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_time (
    time_key SERIAL PRIMARY KEY,
    ts TIMESTAMPTZ UNIQUE NOT NULL,
    day DATE NOT NULL,
    hour INT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_events (
    event_key BIGSERIAL PRIMARY KEY,
    event_id UUID UNIQUE NOT NULL,
    user_id INT NOT NULL,
    product_id INT NOT NULL,
    action TEXT NOT NULL,
    quantity INT NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    revenue NUMERIC(12,2) NOT NULL,
    event_ts TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gold_kpis_5m (
    window_start TIMESTAMPTZ PRIMARY KEY,
    window_end TIMESTAMPTZ NOT NULL,
    active_users INT NOT NULL,
    orders INT NOT NULL,
    revenue NUMERIC(12,2) NOT NULL,
    conversion_rate NUMERIC(8,4) NOT NULL,
    avg_order_value NUMERIC(12,2) NOT NULL,
    loaded_at TIMESTAMPTZ DEFAULT NOW()
);
