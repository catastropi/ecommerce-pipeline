"""응답 형태를 명시적으로 잡아주는 Pydantic 스키마들. Swagger 문서에도 그대로 반영된다."""
from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class DailySales(BaseModel):
    order_date: date
    total_revenue: float
    order_count: int
    revenue_change_pct: Optional[float] = None
    running_total_revenue: Optional[float] = None
    moving_avg_revenue: Optional[float] = None


class MonthlySales(BaseModel):
    order_year: int
    order_month: int
    total_revenue: float
    order_count: int
    revenue_change_pct: Optional[float] = None


class CategorySales(BaseModel):
    product_category_name_english: str
    total_revenue: float
    order_count: int
    item_count: int


class TopProduct(BaseModel):
    product_id: str
    product_category_name_english: Optional[str] = None
    total_revenue: float
    order_count: int


class CustomerSummary(BaseModel):
    customer_unique_id: str
    order_count: int
    total_order_value: float
    avg_order_value: float
    is_repeat_customer: bool
    cltv_estimate: float


class OrderSummary(BaseModel):
    order_id: str
    customer_id: Optional[str] = None
    order_date: Optional[date] = None
    order_status: Optional[str] = None
    item_total_value: float


class KpiSnapshot(BaseModel):
    fact_row_count: Optional[int] = None
    repurchase_rate: Optional[float] = None
    avg_order_value: Optional[float] = None
    avg_delivery_time_days: Optional[float] = None
    refund_rate: Optional[float] = None
    computed_at: Optional[datetime] = None


# ---------------------------------------------------------------------------
# POST /ingest/orders 요청 스키마.
# 배치와 별개로 주문 이벤트를 하나씩 받아 스테이징하는 엔드포인트용.
# incremental_ingest.py / dags/scripts/incremental_ingest.py 참고.
# ---------------------------------------------------------------------------
class OrderItemIn(BaseModel):
    product_id: str
    seller_id: str
    price: float = Field(gt=0)
    freight_value: float = Field(default=0.0, ge=0)
    shipping_limit_date: Optional[str] = None


class PaymentIn(BaseModel):
    payment_type: str
    payment_installments: int = Field(default=1, ge=1)
    payment_value: float = Field(gt=0)
    payment_sequential: int = Field(default=1, ge=1)


class OrderEventIn(BaseModel):
    customer_id: str
    order_id: Optional[str] = None
    order_status: str = "created"
    order_purchase_timestamp: Optional[str] = None
    items: List[OrderItemIn] = Field(min_length=1)
    payment: Optional[PaymentIn] = None


class OrderEventAck(BaseModel):
    order_id: str
    staged_items: int
    staged_payment: bool
    message: str = "다음 배치 실행 시 원본 CSV에 병합됩니다."
