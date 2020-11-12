########################################## Libraries ##########################################
import pandas as pd
import requests
import sqlite3
from bs4 import BeautifulSoup

########################################## 종목코드 불러오기 ##########################################
# 한국거래소에서 최신 종목코드 불러옴
code_df = pd.read_html('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13', header=0)[0] 
# 종목코드가 6자리이기 때문에 6자리를 맞춰줌
code_df.종목코드 = code_df.종목코드.map('{:06d}'.format) 
code_df = code_df[['종목코드']] 

########################################## 일봉 데이터 크롤링 ##########################################
# list 생성
result = {'종목코드': [], '날짜': [], '종가': [], '시가': [], '고가': [], '저가': [], '거래량': []}
# sqllite3 DB연결
con = sqlite3.connect("C:/Users/kdy88/OneDrive/바탕 화면/Data Analysis/kospi.db")
# 종목코드 반복
for c in range(len(code_df)): # 2354가 끝
    page_num = 2 # 600페이지가 최대
    # 페이지 no반복
    for n in range(page_num):
        code = code_df['종목코드'][c]
        url = 'https://finance.naver.com/item/sise_day.nhn?code='+code+'&page='+str(n+1)
        r = requests.get(url)
        html = r.content
        soup = BeautifulSoup(html, 'html.parser') 
        tr = soup.select('table > tr')

        # tr > td 반복                  
        for i in range(1, len(tr)-1):
            if tr[i].select('td')[0].text.strip():
                result['종목코드'].append(code)
                result['날짜'].append(tr[i].select('td')[0].text.strip())
                result['종가'].append(tr[i].select('td')[1].text.strip())
                result['시가'].append(tr[i].select('td')[3].text.strip())
                result['고가'].append(tr[i].select('td')[4].text.strip())
                result['저가'].append(tr[i].select('td')[5].text.strip())
                result['거래량'].append(tr[i].select('td')[6].text.strip())
    print('{}번째 종목 업데이트가 완료되었습니다.'.format(c))

########################################## DB저장 ##########################################
# Data frame
df = pd.DataFrame(result)
# 중복 제거
df.drop_duplicates(subset=['종목코드', '날짜'], keep = 'first', inplace=True)
# DB로 전송
df.to_sql(name = 'stock_daily_ori', con = con, if_exists = 'append')
 