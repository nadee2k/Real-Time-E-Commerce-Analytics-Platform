-- Star schema for e-commerce analytics warehouse

-- KPI mart table for 5-minute rolling windows
CREATE TABLE IF NOT EXISTS gold_kpis_5m (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    active_users INTEGER NOT NULL,
    orders INTEGER NOT NULL,
    revenue DECIMAL(10,2) NOT NULL,
    conversion_rate DECIMAL(5,4) NOT NULL,
    avg_order_value DECIMAL(10,2) NOT NULL,
    loaded_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (window_start)
);

-- Optional: Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_gold_kpis_5m_window_end ON gold_kpis_5m (window_end);
CREATE INDEX IF NOT EXISTS idx_gold_kpis_5m_loaded_at ON gold_kpis_5m (loaded_at);