# ====================================================================================

from fastapi import FastAPI, HTTPException
from typing import Dict
from pydantic import BaseModel
import yfinance as yf
from datetime import datetime
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
from fastapi.responses import JSONResponse
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
import aioredis
import FinanceDataReader as fdr
# Firebase Admin 초기화
cred = credentials.Certificate("credentials.json")  # Firestore credentials
firebase_admin.initialize_app(cred)

# Firestore 클라이언트
db = firestore.client()

app = FastAPI()


class Stock(BaseModel):
    symbol: str
    recommendation_reason: str
    target_return: str
    recommendation_date: str  # 추천 날짜 필드 추가
    ing: str
    country: str  # 국가 필드 추가


@app.on_event("startup")
async def startup():
    # Redis 캐시 초기화
    redis = aioredis.create_redis_pool("redis://localhost:6379",
                                       encoding="utf8")
    FastAPICache.init(RedisBackend(redis), prefix="fastapi-cache")


async def get_stock_from_firestore(symbol: str, country: str) -> Dict:
    """Firestore에서 주식 정보 조회"""
    collection_name = f'stockRecommendations{country.upper()}'  # 컬렉션 이름 결정
    doc_ref = db.collection(collection_name).document(symbol)
    doc = doc_ref.get()
    if doc.exists:
        return doc.to_dict()
    else:
        return None


@app.get("/stocks/{country}")
async def list_stocks(country: str):
    """Firestore에서 모든 주식 종목과 기본 정보 반환"""
    stocks_col_ref = db.collection('stockRecommendations' + str(country))
    stocks_docs = stocks_col_ref.stream()

    stocks = {}
    for doc in stocks_docs:
        stocks[doc.id] = doc.to_dict()

    return JSONResponse(content=stocks)


@app.get("/stocks/{country}/{symbol}")
async def get_stock_info(symbol: str, country: str):
    stock_info = await get_stock_from_firestore(symbol, country)
    if not stock_info:
        raise HTTPException(status_code=404, detail="Stock not found")

    recommendation_date = datetime.strptime(
        stock_info['recommendation_date'], "%Y-%m-%d")
    # 데이터 확인을 위해 종료 날짜를 시작 날짜로부터 5일 후로 설정
    end_date = recommendation_date + pd.Timedelta(days=5)

    if country == 'US':
        temp = yf.download(symbol, start=recommendation_date.strftime(
            "%Y-%m-%d"))['Close']
    else:
        temp = fdr.DataReader(
            symbol, start=recommendation_date)['Close']

    if temp.empty:
        raise HTTPException(
            status_code=404, detail="No historical data available")

    temp.dropna(how="any", inplace=True)

    # 첫 번째와 마지막 close 값에 대한 안전한 접근을 위해 .iloc 사용
    recommendation_close = float(temp.iloc[0])
    current_close = float(temp.iloc[-1])
    return_rate = ((current_close - recommendation_close) /
                   recommendation_close) * 100

    price_dict = {date.strftime(
        "%Y-%m-%d"): price for date, price in temp.items()}

    return JSONResponse(content={
        "symbol": stock_info['company_name'],
        "last_close": current_close,
        "recommendation_close": recommendation_close,
        "return_rate": return_rate,
        "recommendation_reason": stock_info['recommendation_reason'],
        "target_return": stock_info['target_return'],
        "recommendation_date": stock_info['recommendation_date'],
        "ing": stock_info['ing'],
        "country": country,
        "price": price_dict
    })
