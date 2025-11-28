-- Схемы
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS ods;
CREATE SCHEMA IF NOT EXISTS dm;

CREATE TABLE IF NOT EXISTS stg.coffee_sales (
    id BIGSERIAL PRIMARY KEY,
    sale_date DATE,
    sale_datetime TIMESTAMPTZ NOT NULL,
    hour_of_day SMALLINT,
    payment_type VARCHAR(10),
    card_anon_hash TEXT,
    revenue_rub NUMERIC(10,2),
    coffee_name TEXT,
    time_of_day TEXT,
    weekday TEXT,
    month_name TEXT,
    weekdaysort SMALLINT,
    monthsort SMALLINT,
    load_date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    src_name TEXT NOT NULL,
    row_hash TEXT NOT NULL
);

-- Уникальность: один источник, одна дата+время, один хеш — одна запись
CREATE UNIQUE INDEX IF NOT EXISTS idx_stg_uniq ON stg.coffee_sales (src_name, sale_datetime, row_hash);

-- ods
CREATE TABLE IF NOT EXISTS ods.coffee_sales_clean (
    id BIGSERIAL PRIMARY KEY,
    sale_date DATE NOT NULL,
    sale_datetime TIMESTAMPTZ NOT NULL,
    hour_of_day SMALLINT CHECK (hour_of_day BETWEEN 0 AND 23),
    payment_type VARCHAR(10) CHECK (payment_type IN ('cash', 'card')),
    card_anon_hash TEXT,
    revenue_rub NUMERIC(10,2) NOT NULL CHECK (revenue_rub > 0),
    coffee_name TEXT NOT NULL,
    load_date TIMESTAMPTZ NOT NULL,
    src_row_hash TEXT NOT NULL UNIQUE,
    src_name TEXT NOT NULL
);

-- dm
CREATE TABLE IF NOT EXISTS dm.coffee_sales_daily (
    sale_date DATE PRIMARY KEY,
    total_revenue_rub NUMERIC(12,2) NOT NULL,
    avg_check_rub NUMERIC(8,2) NOT NULL,
    total_units_sold BIGINT NOT NULL,
    top_hour SMALLINT NOT NULL CHECK (top_hour BETWEEN 0 AND 23),
    top_hour_sales BIGINT NOT NULL,
    loaded_at TIMESTAMPTZ DEFAULT NOW()
);