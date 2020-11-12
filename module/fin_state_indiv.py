############################################ Libraries ############################################
import pandas as pd
import requests
import sqlite3
import numpy
from bs4 import BeautifulSoup
from selenium import webdriver

######################################### 종목코드 불러오기 #########################################
# 한국거래소에서 최신 종목코드 불러옴
code_df = pd.read_html('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13', header=0)[0] 
# 종목코드가 6자리이기 때문에 6자리를 맞춰줌
code_df.종목코드 = code_df.종목코드.map('{:06d}'.format) 
code_df = code_df[['종목코드']] 

########################################## 재무제표 크롤링 ##########################################
# sqllite3 DB연결
con = sqlite3.connect("C:/Users/kdy88/OneDrive/바탕 화면/Data Analysis/kospi.db")

# Selenium으로 Itooza에 직접 로그인
driver = webdriver.Chrome('C:\chromedriver.exe')
driver.get('https://login.itooza.com/login.htm')
# id, pw전송
driver.find_element_by_name('txtUserId').send_keys('kdy88he')
driver.find_element_by_name('txtPassword').send_keys('ehddbs1021')
# 로그인 버튼 클릭
login_btn = driver.find_element_by_css_selector('#login-container-01 > div.boxBody > div.leftCol > div.login-box-wrap > div.btn-login > input[type=image]')
login_btn.click()

# 데이터 수집
# 종목코드에 따른 URL
code = '005930'
url = 'http://www.itooza.com/vclub/y10_page.php?cmp_cd='+code+'&mode=db&ss=10&sv=1&lsmode=1&lkmode=2&pmode=1&exmode=1&accmode=1'
# Selenium을 통한 로그인 세션을 유지
r = driver.get(url)
html = driver.page_source
soup = BeautifulSoup(html, 'html.parser')
# 첫 번째 열(날짜)과 나머지를 구분
tr1 = soup.select('table#y10_tb_1 > tbody > tr')
tr2 = soup.select('table#y10_tb_2 > tbody > tr')
# list 생성
result = {}

try:
    # header 수집
    header = soup.select('table#y10_tb_2 > thead > tr > th')
    for i in range(1, len(header)):
        result[header[i].text.strip()] = []
    result['재무상태표'] = [] #재무상태표는 따로 추가

    # 행 반복
    for i in range(len(tr1)):

        # 열 반복
        if tr1[i]['id'].count('-') == 1: #숨겨진 행은 제외
            column_first = len(tr1[1].select('th'))
            column_other = len(tr2[1].select('td'))
            for j in range(column_first):
                result['재무상태표'].append(tr1[i].select('th')[j].text.strip())
            for k in range(column_other):
                result[str(list(result.keys())[k])].append(tr2[i].select('td')[k].text.strip())
        else:
            pass
    
    # Data Frame
    df = pd.DataFrame(result)
    # 컬럼 순서 변경
    df = df.reindex(columns = sorted(df.columns))
    df = df.reindex(columns = (['재무상태표'] + list([a for a in df.columns if a != '재무상태표'])))
    # Transpose
    df = df.T
    # 종목코드 컬럼
    df['종목코드'] = code
except Exception as e:
    print('에러명 : {}, URL : {}'.format(e, url))
    pass
########################################## DB저장 ##########################################
# 결측치 처리
df = df.replace('N/A', 0)

# 컬럼명 재할당 및 수정
df.columns = df.iloc[0].replace(" ","")
df.rename(columns = {'005930' : '종목코드'}, inplace = True)

# 중복 또는 필요없는 컬럼 삭제
df = df.drop('기타', axis = 1)
df = df.drop('재무상태표')
# DB로 전송
df.to_sql(name = 'fin_state3', con = con, if_exists = 'append') 