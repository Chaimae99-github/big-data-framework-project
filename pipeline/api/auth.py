from datetime import datetime, timedelta
from jose import jwt

SECRET_KEY = "supersecret"
ALGORITHM = "HS256"

def create_token(username: str):
    payload = {
        "sub": username,
        "exp": datetime.utcnow() + timedelta(hours=2)
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
