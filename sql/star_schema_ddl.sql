-- Star Schema DDL
-- fact_sales는 order_items grain(주문 내 상품 한 줄)을 기준으로 하고
-- dim_* 테이블과 FK로 연결된다.

CREATE SCHEMA IF NOT EXISTS mart;

-- ----------------------------------------------------------------------------
-- dim_customer
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id             VARCHAR(64) PRIMARY KEY,
    customer_unique_id      VARCHAR(64) NOT NULL,
    customer_zip_code_prefix INT,
    customer_city           VARCHAR(100),
    customer_state          VARCHAR(2),
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- dim_product
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_product (
    product_id                     VARCHAR(64) PRIMARY KEY,
    product_category_name          VARCHAR(100),
    product_category_name_english  VARCHAR(100),
    product_weight_g               NUMERIC(10, 2),
    product_length_cm              NUMERIC(10, 2),
    product_height_cm              NUMERIC(10, 2),
    product_width_cm               NUMERIC(10, 2),
    updated_at                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- dim_seller
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id               VARCHAR(64) PRIMARY KEY,
    seller_zip_code_prefix   INT,
    seller_city              VARCHAR(100),
    seller_state             VARCHAR(2),
    updated_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- dim_date : 날짜 차원. 요일/주말 여부는 미리 계산해서 저장.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_id       DATE PRIMARY KEY,
    year          INT NOT NULL,
    month         INT NOT NULL,
    day           INT NOT NULL,
    day_of_week   INT NOT NULL,      -- 1=월요일 ... 7=일요일
    is_weekend    BOOLEAN NOT NULL
);

-- ----------------------------------------------------------------------------
-- fact_payment : 주문 하나에 결제가 여러 건(분할 결제 등) 있을 수 있어서
-- order_id 기준으로 fact_sales 와 조인해서 쓴다. grain이 order item이 아니라
-- 결제 건 단위(order_id + payment_sequential)라 dim이 아니라 별도 fact로 뒀다.
-- 예전에는 dim_payment라는 이름이었는데, 결제 자체가 measure(payment_value)를
-- 가진 사건에 가까워서 이름을 fact_payment로 바꿨다.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_payment (
    payment_id            SERIAL PRIMARY KEY,
    order_id              VARCHAR(64) NOT NULL,
    payment_sequential    INT,
    payment_type          VARCHAR(30),
    payment_installments  INT,
    payment_value         NUMERIC(10, 2),
    UNIQUE (order_id, payment_sequential)
);

-- ----------------------------------------------------------------------------
-- fact_sales : order_items grain. 실제 매출/배송 지표는 여기서 계산된다.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_sales (
    order_id           VARCHAR(64) NOT NULL,
    order_item_id       INT NOT NULL,
    customer_id         VARCHAR(64) REFERENCES dim_customer(customer_id),
    product_id          VARCHAR(64) REFERENCES dim_product(product_id),
    seller_id           VARCHAR(64) REFERENCES dim_seller(seller_id),
    order_date          DATE REFERENCES dim_date(date_id),
    order_status        VARCHAR(30),
    price               NUMERIC(10, 2),
    freight_value       NUMERIC(10, 2),
    item_total_value    NUMERIC(10, 2),
    delivery_days       BIGINT,
    ingested_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id, order_item_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_order_date ON fact_sales(order_date);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON fact_sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON fact_sales(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_seller ON fact_sales(seller_id);

-- ----------------------------------------------------------------------------
-- 고객 단위 피처 테이블 (Star Schema 정석은 아니지만, 대시보드/API가
-- 매번 재계산하지 않도록 Spark에서 만든 결과를 그대로 적재해두는 서빙 테이블)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customer_features (
    customer_unique_id   VARCHAR(64) PRIMARY KEY,
    order_count          BIGINT,
    total_order_value    NUMERIC(12, 2),
    avg_order_value      NUMERIC(10, 2),
    first_purchase_at    TIMESTAMP,
    last_purchase_at     TIMESTAMP,
    is_repeat_customer   BOOLEAN,
    first_purchase_flag  BOOLEAN,
    cltv_estimate        NUMERIC(12, 2),
    updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- mart 스키마: Spark 집계 결과를 그대로 적재하는 서빙 테이블들.
-- 배치마다 truncate 후 통째로 다시 적재한다.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart.daily_sales (
    order_date            DATE PRIMARY KEY,
    total_revenue          NUMERIC(14, 2),
    order_count             BIGINT,
    prev_day_revenue        NUMERIC(14, 2),
    revenue_change_pct      NUMERIC(8, 2),
    running_total_revenue   NUMERIC(16, 2),
    moving_avg_revenue      NUMERIC(14, 2)
);

CREATE TABLE IF NOT EXISTS mart.monthly_sales (
    order_year             INT,
    order_month            INT,
    total_revenue           NUMERIC(14, 2),
    order_count             INT,
    prev_month_revenue      NUMERIC(14, 2),
    revenue_change_pct      NUMERIC(8, 2),
    PRIMARY KEY (order_year, order_month)
);

CREATE TABLE IF NOT EXISTS mart.category_sales (
    product_category_name_english   VARCHAR(100) PRIMARY KEY,
    total_revenue                    NUMERIC(14, 2),
    order_count                      BIGINT,
    item_count                       BIGINT
);

CREATE TABLE IF NOT EXISTS mart.region_sales (
    customer_state    VARCHAR(2) PRIMARY KEY,
    total_revenue      NUMERIC(14, 2),
    order_count         INT,
    customer_count      INT
);

CREATE TABLE IF NOT EXISTS mart.seller_sales (
    seller_id           VARCHAR(64) PRIMARY KEY,
    seller_state         VARCHAR(2),
    total_revenue         NUMERIC(14, 2),
    order_count           INT,
    revenue_rank          INT,
    revenue_dense_rank    INT
);

CREATE TABLE IF NOT EXISTS mart.top_products (
    product_id                     VARCHAR(64) PRIMARY KEY,
    product_category_name_english  VARCHAR(100),
    total_revenue                   NUMERIC(14, 2),
    order_count                     INT
);

CREATE TABLE IF NOT EXISTS mart.best_product_per_category (
    product_category_name_english  VARCHAR(100) PRIMARY KEY,
    product_id                     VARCHAR(64),
    total_revenue                   NUMERIC(14, 2),
    order_count                     INT
);

CREATE TABLE IF NOT EXISTS mart.hourly_orders (
    order_hour     INT PRIMARY KEY,
    order_count     INT
);

-- 배치 실행마다 갱신되는 헤드라인 KPI 스냅샷 (재구매율/평균 주문금액/평균 배송일/환불률)
CREATE TABLE IF NOT EXISTS mart.kpi_snapshot (
    id                      SERIAL PRIMARY KEY,
    fact_row_count           BIGINT,
    repurchase_rate          NUMERIC(6, 4),
    avg_order_value          NUMERIC(10, 2),
    avg_delivery_time_days   NUMERIC(6, 2),
    refund_rate              NUMERIC(6, 4),
    computed_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
