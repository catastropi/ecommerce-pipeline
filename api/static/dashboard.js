// 대시보드 화면에서 쓰는 fetch + Chart.js 초기화 로직.
// 서버사이드 렌더링 없이, 페이지가 뜬 뒤 각 엔드포인트를 순서대로 불러와 채운다.

const BRL = new Intl.NumberFormat("pt-BR", { style: "currency", currency: "BRL" });

// dashboard.css의 CSS 변수와 맞춘 차트 색상 팔레트. 로직과 무관한 스타일 상수.
const PALETTE = {
    accent: "#2c4a6e",
    accent2: "#c97a3d",
    accent3: "#3f6b5c",
    accent4: "#7a5980",
    categorySet: ["#2c4a6e", "#c97a3d", "#3f6b5c", "#7a5980", "#6b8caf", "#e0a672", "#7fa89a", "#a687ac"],
};

Chart.defaults.font.family = "'IBM Plex Sans', sans-serif";
Chart.defaults.color = "#5b6572";
Chart.defaults.borderColor = "#dee1e4";

async function fetchJson(url) {
    const res = await fetch(url);
    if (!res.ok) {
        console.warn(`요청 실패: ${url} (${res.status})`);
        return null;
    }
    return res.json();
}

async function loadKpis() {
    const [kpi, latestDaily] = await Promise.all([
        fetchJson("/kpis"),
        fetchJson("/sales/daily?limit=1"),
    ]);
    if (!kpi) return;

    // kpi_snapshot에는 총 매출 컬럼이 없어서, mart.daily_sales의 running_total_revenue
    // (order_date 기준 누적 합계) 마지막 값을 총 매출로 사용한다. avg_order_value(주문 단위)와
    // fact_row_count(order item 단위)는 grain이 달라 곱해서 쓰면 안 된다.
    const totalRevenue = latestDaily && latestDaily.length ? latestDaily[0].running_total_revenue : null;
    document.getElementById("kpi-revenue").innerText = totalRevenue != null ? BRL.format(totalRevenue) : "-";
    document.getElementById("kpi-aov").innerText = kpi.avg_order_value ? BRL.format(kpi.avg_order_value) : "-";
    document.getElementById("kpi-repurchase").innerText = kpi.repurchase_rate
        ? `${(kpi.repurchase_rate * 100).toFixed(1)}%`
        : "-";
    document.getElementById("kpi-delivery").innerText =
        kpi.avg_delivery_time_days != null
            ? `${kpi.avg_delivery_time_days.toFixed(1)}일 / ${((kpi.refund_rate || 0) * 100).toFixed(1)}%`
            : "-";
}

async function loadDailySalesChart() {
    const data = await fetchJson("/sales/daily?limit=60");
    if (!data || data.length === 0) return;

    new Chart(document.getElementById("dailySalesChart"), {
        type: "line",
        data: {
            labels: data.map((d) => d.order_date),
            datasets: [
                {
                    label: "일 매출",
                    data: data.map((d) => d.total_revenue),
                    borderColor: PALETTE.accent,
                    backgroundColor: "rgba(44,74,110,0.12)",
                    tension: 0.25,
                    yAxisID: "y",
                },
                {
                    label: "7일 이동평균",
                    data: data.map((d) => d.moving_avg_revenue),
                    borderColor: PALETTE.accent2,
                    borderDash: [6, 4],
                    tension: 0.25,
                    yAxisID: "y",
                },
            ],
        },
        options: { responsive: true, scales: { y: { beginAtZero: true } } },
    });
}

async function loadCategoryChart() {
    const data = await fetchJson("/sales/category");
    if (!data || data.length === 0) return;
    const top = data.slice(0, 8);

    new Chart(document.getElementById("categoryChart"), {
        type: "doughnut",
        data: {
            labels: top.map((d) => d.product_category_name_english),
            datasets: [{ data: top.map((d) => d.total_revenue), backgroundColor: PALETTE.categorySet }],
        },
        options: { responsive: true },
    });
}

async function loadRegionChart() {
    const data = await fetchJson("/sales/region");
    if (!data || data.length === 0) return;

    new Chart(document.getElementById("regionChart"), {
        type: "bar",
        data: {
            labels: data.map((d) => d.customer_state),
            datasets: [{ label: "매출", data: data.map((d) => d.total_revenue), backgroundColor: PALETTE.accent3 }],
        },
        options: { responsive: true, indexAxis: "y" },
    });
}

async function loadHourlyChart() {
    const data = await fetchJson("/sales/hourly");
    if (!data || data.length === 0) return;

    new Chart(document.getElementById("hourlyChart"), {
        type: "bar",
        data: {
            labels: data.map((d) => `${d.order_hour}시`),
            datasets: [{ label: "주문 건수", data: data.map((d) => d.order_count), backgroundColor: PALETTE.accent4 }],
        },
        options: { responsive: true },
    });
}

async function loadTopProductsTable() {
    const data = await fetchJson("/top-products?limit=10");
    const tbody = document.querySelector("#topProductsTable tbody");
    if (!data || !tbody) return;

    tbody.innerHTML = data
        .map(
            (row) => `
        <tr>
            <td><code>${row.product_id}</code></td>
            <td>${row.product_category_name_english ?? "-"}</td>
            <td class="num">${BRL.format(row.total_revenue)}</td>
        </tr>`
        )
        .join("");
}

(async function init() {
    await Promise.allSettled([
        loadKpis(),
        loadDailySalesChart(),
        loadCategoryChart(),
        loadRegionChart(),
        loadHourlyChart(),
        loadTopProductsTable(),
    ]);
})();
