import pandas as pd

def stock_code():
    # 종목코드를 불러옴
    code_df = pd.read_html('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13', header=0)[0]

    # 종목코드가 6자리므로 맞춰줌
    code_df.종목코드 = code_df.종목코드.map('{:06d}'.format)
    code_df = code_df[['종목코드']]
    print(code_df)
