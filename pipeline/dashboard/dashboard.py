import requests
import pandas as pd
from dash import Dash, html, dcc
import plotly.express as px
import requests


# ==============================
# CONFIG API
# ==============================
API_URL = "http://fastapi-gold:8000"

# ⚠️ username/password en QUERY (comme ton API attend)
login = requests.post(
    f"{API_URL}/login?username=admin&password=admin"
)

print(login.text)  # debug

token = login.json()["access_token"]
headers = {"Authorization": f"Bearer {token}"}

# ==============================
# RECUP DATA API
# ==============================

top_products = requests.get(
    f"{API_URL}/gold/top-products?page=1&size=20",
    headers=headers
).json()

monthly_sales = requests.get(
    f"{API_URL}/gold/monthly-sales?page=1&size=50",
    headers=headers
).json()

customer_ltv = requests.get(
    f"{API_URL}/gold/customer-lifetime?page=1&size=20",
    headers=headers
).json()

df_top = pd.DataFrame(top_products)
df_monthly = pd.DataFrame(monthly_sales)
df_ltv = pd.DataFrame(customer_ltv)

# ==============================
# GRAPHIQUES
# ==============================

fig_top = px.bar(
    df_top,
    x="product_name",
    y="total_sales",
    title="Top Products"
)

fig_monthly = px.line(
    df_monthly,
    x="month",
    y="total_sales",
    title="Monthly Sales"
)

fig_ltv = px.bar(
    df_ltv,
    x="customer_no",
    y="total_sales",
    title="Customer Lifetime Value"
)

# ==============================
# DASH APP
# ==============================

app = Dash(__name__)

app.layout = html.Div([
    html.H1("Dashboard Datamart Gold"),

    dcc.Graph(figure=fig_top),
    dcc.Graph(figure=fig_monthly),
    dcc.Graph(figure=fig_ltv),
])

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)

