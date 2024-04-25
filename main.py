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
import numpy as np

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


# 최상단에 글로벌 변수 선언
results = {}

async def fetch_financial_metrics_from_firestore(symbol: str, country: str):
    """Firestore에서 금융 지표 데이터를 가져옵니다."""
    collection_name = f'stockRecommendations{country.upper()}FinancialMetrics'
    doc_ref = db.collection(collection_name).document(symbol)
    doc = doc_ref.get()
    if doc.exists:
        return doc.to_dict()
    else:
        return None

async def store_financial_metrics_to_firestore(symbol: str, country: str, metrics: dict):
    """Firestore에 금융 지표 데이터를 저장합니다."""
    collection_name = f'stockRecommendations{country.upper()}FinancialMetrics'
    doc_ref = db.collection(collection_name).document(symbol)
    doc_ref.set(metrics)

async def calculate_and_store_financial_metrics(symbol: str, country: str):
    """금융 지표를 계산하고 Firestore에 저장합니다."""
    metrics = await calculate_financial_metrics(symbol)
    await store_financial_metrics_to_firestore(symbol, country, metrics)
    return metrics

async def calculate_financial_metrics(symbol):
    # Define stock codes and date range
    stock_codes = [symbol]
    start_date = '2023-01-01'  # Start date for a 3-year period
    end_date = datetime.now().strftime('%Y-%m-%d')

    # Determine the market benchmark based on the symbol format
    if symbol.isdigit() and len(symbol) == 6:
        market_index = 'KS11'  # KOSPI index for Korean stocks
    else:
        market_index = '^IXIC'  # NASDAQ Composite Index for others

    # Fetch stock data
    stocks = {code: fdr.DataReader(code, start_date, end_date) for code in stock_codes}
    market_returns = fdr.DataReader(market_index, start_date, end_date)['Close'].pct_change().dropna()

    # Calculate daily returns and financial metrics for each stock
    results = {}
    for code, data in stocks.items():
        # Ensure data exists and calculate actual start date
        if not data.empty:
            actual_start_date = data.index.min().strftime('%Y-%m-%d') if data.index.min() > pd.to_datetime(start_date) else start_date
            # Refresh market index data if start date adjustment is needed
            if actual_start_date != start_date:
                market_returns = fdr.DataReader(market_index, actual_start_date, end_date)['Close'].pct_change().dropna()
                
            daily_returns = data['Close'].pct_change().dropna()

            # Calculate financial metrics
            mean_returns = daily_returns.mean() * 252
            std_dev = daily_returns.std() * np.sqrt(252)
            downside_risk = daily_returns[daily_returns < 0].std() * np.sqrt(252)
            sortino_ratio = (mean_returns - 0.03) / downside_risk if downside_risk != 0 else np.nan
            covariance = np.cov(daily_returns, market_returns)[0][1]
            beta = covariance / np.var(market_returns)
            market_mean_return = market_returns.mean() * 252
            alpha = mean_returns - (0.03 + beta * (market_mean_return - 0.03))
            tracking_error = np.std(daily_returns - market_returns) * np.sqrt(252)
            information_ratio = (mean_returns - market_mean_return) / tracking_error if tracking_error != 0 else np.nan
            cum_returns = (1 + daily_returns).cumprod()
            peak = cum_returns.cummax()
            drawdown = (cum_returns - peak) / peak
            max_drawdown = drawdown.min()
            treynor_ratio = (mean_returns - 0.03) / beta if beta != 0 else np.nan
            sharpe_ratio = (mean_returns - 0.03) / std_dev if std_dev != 0 else np.nan

            results = {
                'Sortino Ratio': sortino_ratio,
                'Beta': beta,
                'Alpha': alpha,
                'Information Ratio': information_ratio,
                'Maximum Drawdown': max_drawdown,
                'Treynor Ratio': treynor_ratio,
                'Sharpe Ratio': sharpe_ratio
            }

    return results



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


sector_tickers = {
    "보험업": ["032830", "082640", "000815", "001450", "088350", "000370", "000810", "000545", "005830", "000400", "085620", "000540"],
    "철강금속": ["005490", "009190", "001780", "104700", "058430", "092790", "002220", "005010", "012800", "001940", "014280", "021050", "014285", "001230", "008260", "025820", "306200", "008420", "008350", "007280", "069730", "058650", "069460", "100090", "002690", "000970", "071090", "001080", "139990", "004560", "004890", "032560", "026940", "024090", "010130", "018470", "019440", "000670", "002240", "133820", "103140", "084010", "460850", "009160", "016380", "155660", "012160", "008970", "008355", "006110", "460860", "004020", "001770", "002710", "001430"],
    "기계": ["042700", "454910", "004380", "000490", "267270", "017800", "006570", "049800", "079550", "119650", "002900", "112610", "017960", "120030", "018880", "241560", "210540", "017550", "092440", "017370", "012600", "091090", "058730", "004450", "011700", "000850", "074610", "025890", "012205", "100840", "042670", "012200", "010820", "077970", "015590", "010660", "145210", "034020", "085310", "009310", "007460", "071970"],
    "전기전자": ["007660", "298040", "103590", "353200", "336370", "004490", "017040", "33637K", "007810", "000660", "267260", "009450", "003670", "029530", "00781K", "33637L", "195870", "007815", "020150", "011230", "35320K", "000990", "017900", "000500", "033240", "005680", "005930", "009470", "092220", "011070", "014915", "011690", "004710", "004770", "011930", "008110", "006340", "025540", "005935", "006405", "097520", "014910", "090460", "000300", "109070", "007610", "009140", "008700", "009155", "33626L", "019180", "322000", "006400", "272210", "373220", "044380", "019490", "020760", "066570", "001820", "066575", "066970", "009150", "005870", "006345", "001210", "108320", "248070", "336260", "361610", "001440", "007340", "015260", "034220", "33626K", "192650", "450080"],
    "금융업": ["105560", "036530", "009970", "084695", "084690", "086790", "007860", "071050", "175330", "029780", "071055", "383800", "015860", "078930", "138930", "078935", "00806K", "000070", "000320", "055550", "001800", "001045", "003550", "192400", "005440", "000240", "003030", "006260", "139130", "004150", "000140", "123890", "034730", "000075", "005810", "072710", "026890", "088980", "402340", "006125", "003555", "000590", "008060", "000157", "060980", "107590", "377300", "000155", "034830", "027410", "000150", "38380K", "244920", "229640", "000325", "36328K", "044820", "006040", "006200", "03473K", "006120", "013570", "006840", "000640", "004800", "001040", "002020", "007540", "004990", "363280", "012630", "012320", "138040", "005745", "000145", "267250", "316140", "005740", "002025", "096760", "009540", "008930", "00499K", "009440"],
    "유통업": ["077500", "452260", "02826K", "45014K", "028260", "081660", "002810", "088790", "000050", "009240", "009810", "006880", "002870", "047050", "011810", "118000", "005320", "037710", "381970", "011760", "002700", "001530", "002420", "450140", "007110", "227840", "010420", "122900", "057050", "003010", "339770", "071840", "004080", "093230", "069640", "101140", "008775", "128820", "001740", "123690", "004270", "45226K", "453340", "018670", "017940", "000760", "005360", "001250", "026960", "007070", "000680", "005390", "023530", "282330", "004060", "001120", "069960", "013000", "008770", "139480", "004170", "031430", "111770", "025530", "010600", "005110", "006370", "008600"],
    "증권업": ["039490", "030210", "003540", "003547", "001500", "003545", "005940", "005945", "001275", "001750", "016360", "003470", "030610", "003475", "001270", "003530", "016610", "001725", "00680K", "001510", "003460", "001290", "001720", "003465", "006800", "006805", "001755", "001515", "003535", "001200", "323410", "006220", "024110"],
    "종이목재": ["011280", "002200", "002310", "016590", "004540", "446070", "014160", "009770", "025750", "006740", "004545", "009460", "012690", "213500", "009200", "008250", "001020", "027970"],
    "화학": ["089470", "115390", "025620", "011790", "002380", "014680", "006650", "001570", "004830", "011170", "011500", "006890", "003830", "005720", "051915", "014820", "004835", "035150", "011780", "002840", "051910", "079980", "003720", "055490", "004250", "285130", "002960", "073240", "100250", "015890", "010060", "004000", "000880", "00088K", "178920", "002360", "005420", "008730", "051900", "083420", "051630", "000860", "000885", "007590", "268280", "278470", "005725", "298000", "012610", "011785", "090355", "014530", "003240", "092230", "081000", "226320", "007690", "001550", "002760", "004255", "025860", "001340", "002100", "006380", "298020", "000390", "014825", "004910", "096770", "014440", "078520", "28513K", "003080", "003350", "024890", "120115", "002355", "014830", "000215", "000210", "025000", "006060", "120110", "009830", "272550", "134380", "090350", "282690", "024070", "069260", "009835", "010955", "108675", "004430", "002790", "456040", "096775", "093370", "051905"],
    "운수장비": ["001380", "004105", "002880", "004100", "005387", "033250", "005389", "005385", "012330", "041650", "010770", "005380", "000430", "092200", "009680", "000270", "023800", "200880", "064960", "010580", "006660", "092780", "012280", "005850", "018500", "024900", "214330", "012450", "005030", "271940", "021820", "000040", "009900", "075180", "204320", "023810", "123700", "001420", "001620", "002920", "009320", "013870", "023000", "090080", "047810", "015230", "010690", "011210", "075580", "003620", "010100", "308170", "016740", "042660", "013520", "143210", "378850", "010140", "329180", "064350", "003570"],
    "음식료": ["003230", "001685", "004370", "005180", "008040", "003960", "264900", "145990", "006090", "000890", "004410", "007310", "017810", "027740", "000087", "033920", "023150", "005305", "000080", "003925", "145995", "001790", "002140", "248170", "036580", "002600", "271560", "136490", "005610", "26490K", "006980", "001130", "004415", "005300", "011150", "101530", "014710", "001680", "011155", "003920", "001795", "097950", "003680", "280360"],
    "전기가스": ["003480", "117580", "036460", "004690", "034590", "267290", "015760", "017390", "005090", "071320"],
    "비광속광물": ["000480", "004090", "183190", "014580", "008870", "001520", "000910", "003300", "047400", "462520", "004980", "004985", "004440", "300720", "010040", "344820", "006390", "001525", "011390", "001527", "001560", "007210", "010780", "004870"],
    "의약품": ["000520", "005690", "170900", "003090", "377740", "001060", "009420", "000020", "007575", "001067", "016580", "063160", "000230", "009290", "000105", "207940", "004310", "001630", "002630", "002390", "271980", "002720", "017180", "249420", "214390", "293480", "003850", "007570", "004720", "005500", "000220", "003520", "302440", "102460", "003220", "000227", "185750", "006280", "069620", "033270", "019170", "002210", "003000", "011000", "234080", "068270", "000225", "003060", "001065", "000100", "128940", "019175", "001360"],
    "건설업": ["001470", "002787", "002995", "039570", "009410", "001260", "013700", "002780", "37550K", "126720", "013580", "005960", "013360", "051600", "003075", "002410", "097230", "002990", "010400", "004960", "294870", "002785", "023960", "010960", "047040", "000725", "375500", "006360", "028100", "009415", "000720", "005965", "002460", "034300", "003070"],
    "서비스업": ["100220", "023590", "396690", "395400", "181710", "012510", "357250", "039130", "330590", "357430", "403550", "365550", "031440", "350520", "005250", "032350", "334890", "357120", "451800", "031820", "035250", "015360", "068290", "417310", "448730", "293940", "102260", "020120", "377190", "145270", "034310", "284740", "338100", "192080", "022100", "094280", "058860", "035000", "404990", "078000", "210980", "002030", "003560", "070960", "214320", "072130", "000180", "432320", "030790", "400760", "140910", "088260", "317400", "016880", "034120", "002150", "052690", "030190", "016710", "012030", "053690", "012750", "000700", "035720", "348950", "023350", "037560", "089860", "267850", "005257", "019685", "053210", "018260", "036420", "030000", "307950", "052690", "036570", "019680", "015020", "058850", "035420", "095570", "012170", "114090", "007120", "021240", "002620", "095720", "352820", "204210", "259960", "251270", "011420", "326030"],
    "통신업": ["006490", "030200", "017670", "032640", "126560"]
}

import asyncio

today = datetime.today().strftime('%Y%m%d')

async def fetch_data(ticker, start_date, end_date, market_caps):
    try:
        df = stock.get_market_ohlcv(start_date, end_date, ticker)  # 비동기 버전을 사용해야 함
        if not df.empty and ticker in market_caps.index:
            initial_price = df['종가'].iloc[0]
            final_price = df['종가'].iloc[-1]
            return_rate = (final_price - initial_price) / initial_price * 100
            market_cap = market_caps.loc[ticker]
            return return_rate, market_cap
    except Exception as e:
        print(f"Error retrieving data for {ticker}: {e}")
    return None, None

async def get_weighted_returns(tickers, days, market_caps):
    start_date = (datetime.today() - timedelta(days=days)).strftime('%Y%m%d')
    end_date = datetime.today().strftime('%Y%m%d')
    tasks = [fetch_data(ticker, start_date, end_date, market_caps) for ticker in tickers]
    results = await asyncio.gather(*tasks)
    
    total_market_cap = 0
    weighted_returns = 0
    for result in results:
        return_rate, market_cap = result
        if market_cap is not None:
            total_market_cap += market_cap
            weighted_returns += return_rate * market_cap

    return weighted_returns / total_market_cap if total_market_cap > 0 else None


@app.get("/sector/fetch/{country}")
async def get_all_sectors_from_firestore(country: str) -> Dict[str, Dict]:
    """Firestore에서 주어진 국가의 모든 섹터 주식 정보 조회"""
    collection_name = f'sector{country.upper()}'
    try:
        all_docs = db.collection(collection_name).stream()  # 모든 문서 스트림으로 가져오기
        sectors_data = {doc.id: doc.to_dict() for doc in all_docs if doc.exists}
        return sectors_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch data: {str(e)}")

    if not sectors_data:
        return {}  # 문서가 없을 경우 빈 딕셔너리 반환
    
# Firestore에 데이터 저장하는 함수
async def save_sector_to_firestore(data, country):
    collection_name = f"sector{country.upper()}"
    for sector_name, sector_data in data.items():
        print(f"Attempting to save to collection: {collection_name}, document: {sector_name}")
        doc_ref = db.collection(collection_name).document(sector_name)
        try:
            doc_ref.set(sector_data)
        except Exception as e:
            print(f"Failed to save document: {sector_name} in collection: {collection_name}. Error: {e}")


@app.get("/sector/calculate/{country}")
async def calculate_sector_returns(country: str):
    try:
        market_caps =  stock.get_market_cap(today)  # 비동기 함수 호출에 'await' 사용
        sector_returns = {}
        for sector, tickers in sector_tickers.items():
            # 각 'get_weighted_returns' 호출 앞에 'await' 추가
            sector_returns[sector] = {
                '1 Day': await get_weighted_returns(tickers, 1, market_caps['시가총액']),
                '1 Week': await get_weighted_returns(tickers, 5, market_caps['시가총액']),
                '1 Month': await get_weighted_returns(tickers, 20, market_caps['시가총액']),
                '3 Months': await get_weighted_returns(tickers, 60, market_caps['시가총액']),
                '6 Months': await get_weighted_returns(tickers, 120, market_caps['시가총액']),
                '1 Year': await get_weighted_returns(tickers, 220, market_caps['시가총액'])
            }
        await save_sector_to_firestore(sector_returns, country)  # 이 함수도 비동기로 처리됨
        return JSONResponse(content=sector_returns)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stocks/{country}/{symbol}")
async def get_stock_info(symbol: str, country: str):
    stock_info = await get_stock_from_firestore(symbol, country)
    if not stock_info:
        raise HTTPException(status_code=404, detail="Stock not found")
    
    # Firestore에서 금융 지표를 가져오기 시도
    financial_metrics = await fetch_financial_metrics_from_firestore(symbol, country)
    if not financial_metrics:
        # 금융 지표가 없으면 계산하고 저장
        financial_metrics = await calculate_and_store_financial_metrics(symbol, country)
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

    price_series   = pd.Series(price_data['Close'])

    price_series.index = price_series.index.strftime('%Y-%m-%d')

    # Convert to dictionary if needed
    price_dict = price_series.to_dict()


    if stock_info['ing'] == '성공':
        return_rate = int(str('+') + str(stock_info['target_return']))
    elif stock_info['ing'] == '실패':
        return_rate = float(str('-') + str(float(stock_info['target_return']) / 2) )

    
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
        "financial_metrics": financial_metrics  # 추가된 부분
    })
    
async def update_stock_in_firestore(symbol: str, country: str, updated_info: Dict):
    """Update stock information in Firestore."""
    collection_name = f'stockRecommendations{country.upper()}'
    doc_ref = db.collection(collection_name).document(symbol)
    doc_ref.set(updated_info)  # This overwrites the document with the updated data.
