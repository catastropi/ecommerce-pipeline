// 대시보드 화면에서 쓰는 fetch + Chart.js 초기화 로직.
// 서버사이드 렌더링 없이, 페이지가 뜬 뒤 각 엔드포인트를 순서대로 불러와 채운다.

const BRL = new Intl.NumberFormat("pt-BR", { style: "currency", currency: "BRL" });

async function fetchJson(url) {
    const res = await fetch(url);
    if (!res.ok) {
        console.warn(`요청 실패: ${url} (${res.status})`);
        return null;
    }
    return res.json();
}

async function loadKpis() {
    const kpi = await fetchJson("/kpis");
    if (!kpi) return;

    document.getElementById("kpi-revenue").innerText = kpi.avg_order_value
        ? BRL.format(kpi.fact_row_count ? kpi.avg_order_value * kpi.fact_row_count : kpi.avg_order_value)
        : "-";
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
                    borderColor: "#0d6efd",
                    backgroundColor: "rgba(13,110,253,0.15)",
                    tension: 0.25,
                    yAxisID: "y",
                },
                {
                    label: "7일 이동평균",
                    data: data.map((d) => d.moving_avg_revenue),
                    borderColor: "#fd7e14",
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
            datasets: [{ data: top.map((d) => d.total_revenue) }],
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
            datasets: [{ label: "매출", data: data.map((d) => d.total_revenue), backgroundColor: "#198754" }],
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
            datasets: [{ label: "주문 건수", data: data.map((d) => d.order_count), backgroundColor: "#6610f2" }],
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
            <td class="text-end">${BRL.format(row.total_revenue)}</td>
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
