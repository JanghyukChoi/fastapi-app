# ====================================================================================

from fastapi import FastAPI, HTTPException
from typing import Dict
from pydantic import BaseModel
import yfinance as yf
from datetime import datetime
import pandas as pd
import os
import firebase_admin
from datetime import timedelta
from firebase_admin import credentials, firestore
from fastapi.responses import JSONResponse
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
import aioredis
import json
import FinanceDataReader as fdr
# Firebase Admin 초기화
firebase_credentials = os.getenv('FIREBASE_CREDENTIALS')

# 문자열로 된 인증 정보를 JSON 객체로 변환
cred_dict = json.loads(firebase_credentials)

# Firebase 인증 정보로 변환
cred = credentials.Certificate(cred_dict)

# Firebase 앱 초기화

firebase_admin.initialize_app(cred)

# Firestore 클라이언트
db = firestore.client()

app = FastAPI()

success = 0
fail = 0


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
    
@app.get("/stocks/success/")
async def success_rate():
    statuses = {'진행중': 0, '성공': 0, '실패': 0}
    # 'stockRecommendationsUS'와 'stockRecommendationsKR' 컬렉션에서 데이터 조회
    collections = ['stockRecommendationsUS', 'stockRecommendationsKR']
    
    for collection in collections:
        # 각 컬렉션에서 문서 스트림 가져오기
        docs = db.collection(collection).stream()
        for doc in docs:
            data = doc.to_dict()
            if 'ing' in data:
                status = data['ing']
                if status in statuses:
                    statuses[status] += 1
    
    return JSONResponse(content=statuses)


@app.get("/stocks/{country}")
async def list_stocks(country: str):
    """Firestore에서 모든 주식 종목과 기본 정보 반환"""
    stocks_col_ref = db.collection('stockRecommendations' + str(country))
    stocks_docs = stocks_col_ref.stream()

    stocks = {}
    for doc in stocks_docs:
        stocks[doc.id] = doc.to_dict()

    return JSONResponse(content=stocks)

async def count_stock_statuses():
    statuses = {'진행중': 0, '성공': 0, '실패': 0}
    # 'stockRecommendationsUS'와 'stockRecommendationsKR' 컬렉션에서 데이터 조회
    collections = ['stockRecommendationsUS', 'stockRecommendationsKR']
    
    for collection in collections:
        # 각 컬렉션에서 문서 스트림 가져오기
        docs = db.collection(collection).stream()
        for doc in docs:
            data = doc.to_dict()
            if 'ing' in data:
                status = data['ing']
                if status in statuses:
                    statuses[status] += 1
    
    return statuses


@app.get("/stocks/{country}/{symbol}")
async def get_stock_info(symbol: str, country: str):
    stock_info = await get_stock_from_firestore(symbol, country)
    if not stock_info:
        raise HTTPException(status_code=404, detail="Stock not found")

    recommendation_date = datetime.strptime(stock_info['recommendation_date'], "%Y-%m-%d")
    one_month_later = recommendation_date + timedelta(days=30)
    today = datetime.today()

    if country == 'US':
        price_data = yf.download(symbol, start=recommendation_date.strftime("%Y-%m-%d"), end=today.strftime("%Y-%m-%d"))
    else:
        price_data = fdr.DataReader(symbol, start=recommendation_date, end=today)

    if price_data.empty:
        raise HTTPException(status_code=404, detail="No historical data available")

    price_data.dropna(how="any", inplace=True)
    target_return = float(stock_info['target_return'])
    stop_loss = float('-' + str(float(stock_info['target_return']) / 2))
    recommendation_close = price_data['Close'].iloc[0]

    # 수익률 계산
    price_data['Return'] = (price_data['Close'] - recommendation_close) / recommendation_close * 100

    # 목표 수익률 또는 손절률에 도달하는 첫 날짜를 찾기
    for date, row in price_data.iterrows():
        if row['Return'] >= target_return:
            stock_info['ing'] = '성공'
            break
        elif row['Return'] <= stop_loss:
            stock_info['ing'] = '실패'
            break
    else:
        # If after a month or today's date (whichever is earlier) no goal is achieved
        check_date = min(one_month_later, today)
        last_available_date = price_data.index[-1]
        if last_available_date >= check_date:
            stock_info['ing'] = '실패'
        else:
            stock_info['ing'] = '진행중'

    await update_stock_in_firestore(symbol, country, stock_info)

    current_close = price_data['Close'].iloc[-1]
    return_rate = ((current_close - recommendation_close) / recommendation_close) * 100

    print('Current Close is : ' )
    print(current_close)

    price_series   = pd.Series(price_data['Close'])

    price_series.index = price_series.index.strftime('%Y-%m-%d')

    # Convert to dictionary if needed
    price_dict = price_series.to_dict()


    if stock_info['ing'] == '성공':
        return_rate = int(str('+') + str(stock_info['target_return']))
    elif stock_info['ing'] == '실패':
        return_rate = float(str('-') + str(float(stock_info['target_return']) / 2) )

    else: 
        return_rate =  int(str('+') + str(return_rate))
    
    return JSONResponse(content={
        "symbol": symbol,
        "last_close": float(current_close),
        "recommendation_close": float(recommendation_close),
        "return_rate": return_rate,
        "recommendation_reason": stock_info['recommendation_reason'],
        "target_return": stock_info['target_return'],
        "recommendation_date": stock_info['recommendation_date'],
        "ing": stock_info['ing'],
        "country": country,
        "price" : price_dict,
    })
async def update_stock_in_firestore(symbol: str, country: str, updated_info: Dict):
    """Update stock information in Firestore."""
    collection_name = f'stockRecommendations{country.upper()}'
    doc_ref = db.collection(collection_name).document(symbol)
    doc_ref.set(updated_info)  # This overwrites the document with the updated data.
