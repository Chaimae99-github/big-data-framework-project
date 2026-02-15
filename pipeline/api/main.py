# -*- coding: utf-8 -*-

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from database import get_connection
from auth import create_token
from jose import jwt

app = FastAPI()

security = HTTPBearer()

SECRET_KEY = "supersecret"
ALGORITHM = "HS256"

# ==============================
# AUTH
# ==============================
@app.post("/login")
def login(username: str, password: str):
    # SIMPLE LOGIN DEMO
    if username == "admin" and password == "admin":
        token = create_token(username)
        return {"access_token": token}
    raise HTTPException(status_code=401, detail="Invalid credentials")


def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        return payload["sub"]
    except:
        raise HTTPException(status_code=403, detail="Invalid token")


# ==============================
# GOLD DATAMART ENDPOINTS
# ==============================

@app.get("/gold/top-products")
def get_top_products(
    page: int = Query(1),
    size: int = Query(10),
    user=Depends(verify_token)
):
    offset = (page - 1) * size

    conn = get_connection()
    cur = conn.cursor()

    query = f"""
        SELECT country, product_no, product_name, total_sales, total_quantity, rank
        FROM gold_top_products
        ORDER BY rank
        LIMIT %s OFFSET %s
    """

    cur.execute(query, (size, offset))
    rows = cur.fetchall()

    columns = [desc[0] for desc in cur.description]
    data = [dict(zip(columns, row)) for row in rows]

    cur.close()
    conn.close()

    return data


@app.get("/gold/monthly-sales")
def get_monthly_sales(
    page: int = 1,
    size: int = 10,
    user=Depends(verify_token)
):
    offset = (page - 1) * size

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT year, month, total_sales, total_quantity
        FROM gold_monthly_sales
        ORDER BY year, month
        LIMIT %s OFFSET %s
    """, (size, offset))

    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    data = [dict(zip(columns, row)) for row in rows]

    cur.close()
    conn.close()

    return data


@app.get("/gold/customer-lifetime")
def get_customer_lifetime(
    page: int = 1,
    size: int = 10,
    user=Depends(verify_token)
):
    offset = (page - 1) * size

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM gold_customer_lifetime_value
        ORDER BY total_sales DESC
        LIMIT %s OFFSET %s
    """, (size, offset))

    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    data = [dict(zip(columns, row)) for row in rows]

    cur.close()
    conn.close()

    return data
