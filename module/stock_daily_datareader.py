# Libraries
import urllib.parse
import pandas_datareader.data as web
import pandas as pd
from datetime import datetime
import sqlite3

######################## 종목코드 ########################
# 시장(kospi, kosdaq, konex) 구분
MARKET_CODE_DICT = {
    'kospi': 'stockMkt',
    'kosdaq': 'kosdaqMkt',
    'konex': 'konexMkt'
}
# 최신 종목정보 다운로드
DOWNLOAD_URL = 'kind.krx.co.kr/corpgeneral/corpList.do'
# 시장별 종목정보를 호출하는 함수
def download_stock_codes(market=None, delisted=False):
    params = {'method': 'download'}

    if market.lower() in MARKET_CODE_DICT:
        params['marketType'] = MARKET_CODE_DICT[market]

    if not delisted:
        params['searchType'] = 13

    params_string = urllib.parse.urlencode(params)
    request_url = urllib.parse.urlunsplit(['http', DOWNLOAD_URL, '', params_string, ''])

    stockCode = pd.read_html(request_url, header=0)[0]
    # 6자리 지정
    stockCode.종목코드 = stockCode.종목코드.map('{:06d}'.format)
    # 시장구분 넣기
    stockCode['시장구분'] = market

    return stockCode
# 시장별 종목정보 저장
kospi_code = download_stock_codes('kospi')
kosdaq_code = download_stock_codes('kosdaq')
# 종목코드 형식 맞추기 - konex는 제외
kospi_code['종목코드'] = kospi_code['종목코드']+'.KS'
kosdaq_code['종목코드'] = kosdaq_code['종목코드']+'.KQ'
# 열 병합
all_code = pd.concat([kospi_code, kosdaq_code])
# DB저장
con = sqlite3.connect("C:/Users/kdy88/OneDrive/바탕 화면/Data Analysis/kospi.db")
all_code.to_sql(name = 'stock_code', con = con, if_exists = 'replace')

######################## 일봉 데이터 ########################
# 조회 날짜 설정 
start_time = datetime(2018, 1, 1)
end_time = datetime(2018, 3, 12)
# 종목코드에 따른 데이터 불러오기 len(kospi_code['종목코드'])
for i in range(1, 10):
    try:
        df = web.DataReader(kospi_code['종목코드'][i], "yahoo", start_time, end_time)
        df['종목코드'] = kospi_code['종목코드'][i]
        print(df[i])
    # 종목코드가 없는 경우 에러처리 - 다음 행으로 넘기기
    except Exception as e:
        pass
# DB저장
# sqllite3 연결
# con = sqlite3.connect("C:/Users/kdy88/OneDrive/바탕 화면/Data Analysis/kospi.db")
# DB로 전송
# df.to_sql(name = 'stock_daily', con = con, if_exists = 'replace')