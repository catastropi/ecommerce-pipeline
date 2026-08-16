-- ============================================================================
-- Tableau / 리포팅용 편의 뷰.
--
-- mart.* 테이블들은 이미 Spark에서 계산이 끝난 결과라 그대로 연결해도 되지만,
-- Tableau에서 "판매 상세" 하나로 필터링/드릴다운을 하고 싶을 때를 위해
-- fact_sales + 차원 테이블을 미리 조인해둔 넓은(wide) 뷰를 하나 만들어둔다.
-- ============================================================================

CREATE OR REPLACE VIEW mart.v_sales_detail AS
SELECT
    f.order_id,
    f.order_item_id,
    f.order_date,
    f.order_status,
    f.price,
    f.freight_value,
    f.item_total_value,
    f.delivery_days,
    c.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    p.product_id,
    p.product_category_name_english,
    s.seller_id,
    s.seller_state
FROM fact_sales f
LEFT JOIN dim_customer c ON f.customer_id = c.customer_id
LEFT JOIN dim_product p ON f.product_id = p.product_id
LEFT JOIN dim_seller s ON f.seller_id = s.seller_id;

-- 대시보드 상단 카드에 바로 물릴 수 있는 "가장 최근 배치의 KPI" 뷰
CREATE OR REPLACE VIEW mart.v_latest_kpi AS
SELECT *
FROM mart.kpi_snapshot
ORDER BY computed_at DESC
LIMIT 1;
