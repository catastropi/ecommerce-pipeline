"""
이커머스 데이터 웨어하우스를 조회하는 FastAPI 서버.

FastAPI + Jinja2 + Bootstrap + Chart.js 조합으로 대시보드를 구성했다.
/dashboard는 사람이 보는 화면이고, 나머지 엔드포인트는 그 화면이
fetch()로 불러다 쓰는 JSON API인 동시에 Swagger(/docs)로도 노출된다.
"""
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, Query, Request, status
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import text
from sqlalchemy.orm import Session

from database import get_db
from incremental_ingest import stage_order_event
from schemas import OrderEventAck, OrderEventIn

app = FastAPI(
    title="Ecommerce Data Platform API",
    description="Olist 이커머스 데이터 웨어하우스(Star Schema)를 조회하는 API",
    version="1.0.0",
)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


# ---------------------------------------------------------------------------
# 대시보드 페이지
# ---------------------------------------------------------------------------
@app.get("/dashboard")
def dashboard(request: Request):
    return templates.TemplateResponse(request, "dashboard.html", {})


@app.get("/")
def root():
    return {"message": "대시보드는 /dashboard, API 문서는 /docs 에서 확인하세요."}


# ---------------------------------------------------------------------------
# 주문 이벤트 수집 (배치 파이프라인과 별도 경로)
# Kaggle 데이터셋은 정적 파일이라 파이프라인 자체는 배치로만 동작하는데,
# 여기서 받은 이벤트는 data/raw/incremental/에 CSV로 스테이징해두고
# Airflow의 extract_task가 다음 배치에서 원본 CSV에 병합한다.
# DB에 바로 안 쓰는 이유는, 이 엔드포인트가 웨어하우스 적재 경로가 아니라
# 원본 데이터 소스 역할이기 때문이다. 검증과 정제를 거쳐야 웨어하우스로 간다.
# ---------------------------------------------------------------------------
@app.post("/ingest/orders", response_model=OrderEventAck, status_code=status.HTTP_201_CREATED)
def ingest_order(order: OrderEventIn):
    result = stage_order_event(order.model_dump())
    return OrderEventAck(**result)


# ---------------------------------------------------------------------------
# 핵심 KPI (대시보드 상단 카드용)
# ---------------------------------------------------------------------------
@app.get("/kpis")
def get_kpis(db: Session = Depends(get_db)):
    row = db.execute(text("SELECT * FROM mart.v_latest_kpi")).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="아직 집계된 KPI가 없습니다. 파이프라인을 먼저 실행해주세요.")
    return dict(row)


# ---------------------------------------------------------------------------
# 매출 관련
# ---------------------------------------------------------------------------
@app.get("/sales/daily")
def sales_daily(limit: int = Query(90, le=365), db: Session = Depends(get_db)):
    rows = db.execute(
        text(
            "SELECT * FROM mart.daily_sales ORDER BY order_date DESC LIMIT :limit"
        ),
        {"limit": limit},
    ).mappings().all()
    return list(reversed([dict(r) for r in rows]))


@app.get("/sales/monthly")
def sales_monthly(db: Session = Depends(get_db)):
    rows = db.execute(
        text("SELECT * FROM mart.monthly_sales ORDER BY order_year, order_month")
    ).mappings().all()
    return [dict(r) for r in rows]


@app.get("/sales/category")
def sales_category(db: Session = Depends(get_db)):
    rows = db.execute(
        text("SELECT * FROM mart.category_sales ORDER BY total_revenue DESC")
    ).mappings().all()
    return [dict(r) for r in rows]


@app.get("/sales/region")
def sales_region(db: Session = Depends(get_db)):
    rows = db.execute(
        text("SELECT * FROM mart.region_sales ORDER BY total_revenue DESC")
    ).mappings().all()
    return [dict(r) for r in rows]


@app.get("/sales/hourly")
def sales_hourly(db: Session = Depends(get_db)):
    rows = db.execute(text("SELECT * FROM mart.hourly_orders ORDER BY order_hour")).mappings().all()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# 상품 / 판매자
# ---------------------------------------------------------------------------
@app.get("/top-products")
def top_products(limit: int = Query(10, le=50), db: Session = Depends(get_db)):
    rows = db.execute(
        text("SELECT * FROM mart.top_products ORDER BY total_revenue DESC LIMIT :limit"),
        {"limit": limit},
    ).mappings().all()
    return [dict(r) for r in rows]


@app.get("/sellers/ranking")
def seller_ranking(limit: int = Query(20, le=100), db: Session = Depends(get_db)):
    rows = db.execute(
        text("SELECT * FROM mart.seller_sales ORDER BY revenue_rank LIMIT :limit"),
        {"limit": limit},
    ).mappings().all()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# 고객
# ---------------------------------------------------------------------------
@app.get("/customers")
def list_customers(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, le=100),
    repeat_only: bool = False,
    db: Session = Depends(get_db),
):
    offset = (page - 1) * page_size
    where_clause = "WHERE is_repeat_customer = true" if repeat_only else ""
    rows = db.execute(
        text(
            f"SELECT * FROM customer_features {where_clause} "
            "ORDER BY total_order_value DESC LIMIT :limit OFFSET :offset"
        ),
        {"limit": page_size, "offset": offset},
    ).mappings().all()
    return {"page": page, "page_size": page_size, "items": [dict(r) for r in rows]}


@app.get("/customer/{customer_unique_id}")
def get_customer(customer_unique_id: str, db: Session = Depends(get_db)):
    row = db.execute(
        text("SELECT * FROM customer_features WHERE customer_unique_id = :cid"),
        {"cid": customer_unique_id},
    ).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="해당 고객을 찾을 수 없습니다")

    orders = db.execute(
        text(
            """
            SELECT DISTINCT f.order_id, f.order_date, f.order_status, c.customer_id
            FROM fact_sales f
            JOIN dim_customer c ON f.customer_id = c.customer_id
            WHERE c.customer_unique_id = :cid
            ORDER BY f.order_date DESC
            """
        ),
        {"cid": customer_unique_id},
    ).mappings().all()

    return {"profile": dict(row), "orders": [dict(o) for o in orders]}


# ---------------------------------------------------------------------------
# 주문
# ---------------------------------------------------------------------------
@app.get("/orders")
def list_orders(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, le=100),
    status: Optional[str] = None,
    db: Session = Depends(get_db),
):
    offset = (page - 1) * page_size
    where_clause = "WHERE order_status = :status" if status else ""
    params = {"limit": page_size, "offset": offset}
    if status:
        params["status"] = status

    rows = db.execute(
        text(
            f"""
            SELECT order_id, customer_id, order_date, order_status,
                   SUM(item_total_value) AS item_total_value
            FROM fact_sales
            {where_clause}
            GROUP BY order_id, customer_id, order_date, order_status
            ORDER BY order_date DESC
            LIMIT :limit OFFSET :offset
            """
        ),
        params,
    ).mappings().all()
    return {"page": page, "page_size": page_size, "items": [dict(r) for r in rows]}
