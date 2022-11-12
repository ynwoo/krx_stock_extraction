import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from urllib.request import urlopen, Request

import json, requests 
from datetime import datetime, timedelta

import pymysql # python에서 mysql을 사용하는 패키지
import sqlalchemy # sql 접근 및 관리를 도와주는 패키지
from sqlalchemy import create_engine
from tqdm import tqdm
import FinanceDataReader as fdr

import warnings
warnings.filterwarnings('ignore')


class krx_stock_extraction:
    def __init__(self) -> None:
        pass
    # 1. 주식 종목 수집 및 DB에 저장 (from KRX)
    
    # date generator
    def getDateRange(self, st_date, end_date):
        for n in range(int((end_date - st_date).days)+1):
            yield st_date + timedelta(days=n)

    # 지정한 기간의 KRX 가격 반환
    # mktId: STK(KOSPI), KSQ(KOSDAQ), KNX(KONEX)
    # st_dt, end_dt: 'yyyymmdd'
    def getKRXPrice(self, mktId, st_dt, end_dt):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020103"
        }

        # 수집 기간 설정
        sdate = datetime.strptime(st_dt,'%Y%m%d').date()
        edate = datetime.strptime(end_dt,'%Y%m%d').date()
        dt_idx = []
        for dt in self.getDateRange(sdate, edate):
            if dt.isoweekday() < 6:
                dt_idx.append(dt.strftime("%Y%m%d"))

        daily = []
        for dt in dt_idx:
            # 날짜별 전종목 일봉 불러오기
            p_data = {
                'bld': 'dbms/MDC/STAT/standard/MDCSTAT01501',
                'mktId': mktId,
                'trdDd': dt,
                'share': '1',
                'money': '1',
                'csvxls_isNo': 'false'
            }

            url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
            res = requests.post(url, headers=headers, data=p_data)
            html_text = res.content
            html_json = json.loads(html_text)
            html_jsons = html_json['OutBlock_1']

            if len(html_jsons) > 0:
                for html_json in html_jsons:
                    if html_json['TDD_OPNPRC'] == '-': # 시장이 열리지 않아 값이 없는 경우
                        continue

                    ISU_SRT_CD = html_json['ISU_SRT_CD']
                    ISU_ABBRV = html_json['ISU_ABBRV']
                    TRD_DD = datetime.strptime(dt,'%Y%m%d').strftime('%Y-%m-%d')

                    FLUC_RT = float(html_json['FLUC_RT'].replace(',',''))/100
                    TDD_CLSPRC = int(html_json['TDD_CLSPRC'].replace(',',''))
                    TDD_OPNPRC = int(html_json['TDD_OPNPRC'].replace(',',''))
                    TDD_HGPRC = int(html_json['TDD_HGPRC'].replace(',',''))
                    TDD_LWPRC = int(html_json['TDD_LWPRC'].replace(',',''))

                    ACC_TRDVOL = int(html_json['ACC_TRDVOL'].replace(',',''))
                    ACC_TRDVAL = int(html_json['ACC_TRDVAL'].replace(',',''))
                    MKTCAP = int(html_json['MKTCAP'].replace(',',''))
                    LIST_SHRS = int(html_json['LIST_SHRS'].replace(',',''))

                    daily.append((ISU_SRT_CD,ISU_ABBRV,TRD_DD,TDD_OPNPRC,TDD_HGPRC,TDD_LWPRC,TDD_CLSPRC,ACC_TRDVOL,FLUC_RT,ACC_TRDVAL,MKTCAP,LIST_SHRS))
            else:
                pass

        if len(daily) > 0:
            daily = pd.DataFrame(daily)
            daily.columns = ['stock_code','stock_name','date','open','high','low','close','volume','change','ACC_TRDVAL','MKTCAP','LIST_SHRS']
            daily = daily.sort_values(by='date').reset_index(drop=True)
            return daily

        else:
            return pd.DataFrame()

    # getKRXPrice()함수에서 얻은 정보를 DB에 저장
    # market: 시장구분(kospi, kosdaq, konex)
    def saveKRXPrice(self, pr_df, market, server, user, password, db):
        # sqlalchemy의 create_engine을 이용해 DB 연결
        engine = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(user,password,server,db))

        pr_df.to_sql(name=market+'_unadj',con=engine,if_exists='append',index=False,
             dtype = { # sql에 저장할 때, 데이터 유형도 설정할 수 있다.
                 'stock_code' : sqlalchemy.types.VARCHAR(10),
                 'stock_name' : sqlalchemy.types.TEXT(),
                 'date' : sqlalchemy.types.DATE(),
                 'open' : sqlalchemy.types.BIGINT(),
                 'high' : sqlalchemy.types.BIGINT(),
                 'low' : sqlalchemy.types.BIGINT(),
                 'close' : sqlalchemy.types.BIGINT(),
                 'volume' : sqlalchemy.types.BIGINT(),
                 'change' : sqlalchemy.types.FLOAT(),
                 'ACC_TRDVAL' : sqlalchemy.types.BIGINT(),
                 'MKTCAP' : sqlalchemy.types.BIGINT(),
                 'LIST_SHRS' : sqlalchemy.types.BIGINT()                 
             }
            )
        engine.dispose()

    # 2. 수정주가 수집 및 db에 저장(from FDR)
    # mktId: STK(KOSPI), KSQ(KOSDAQ), KNX(KONEX)
    # st_dt, end_dt: 'yyyy-mm-dd'
    def get_adjusted_KRXPrice(self, mktId, st_dt, end_dt):
        stock_list = fdr.StockListing(mktId).dropna()

        daily = pd.DataFrame()
        for code, name in tqdm(stock_list[['Symbol', 'Name']].values):
            ohlcv = fdr.DataReader(code, st_dt, end_dt)
            ohlcv['Code'] = code
            ohlcv['Name'] = name
            daily= pd.concat([daily, ohlcv])
        
        daily['Date'] = daily.index
        daily = daily.reset_index(drop=True)

        return daily

    # get_adjusted_KRXPrice()함수에서 얻은 정보를 DB에 저장
    # market: 시장구분(kospi, kosdaq, konex)
    def save_adjusted_KRXPrice(self, pr_df, market, server, user, password, db):
        # sqlalchemy의 create_engine을 이용해 DB 연결
        engine = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(user,password,server,db))

        pr_df.to_sql(name=market+'_adj',con=engine,if_exists='append',index=False,
             dtype = { # sql에 저장할 때, 데이터 유형도 설정할 수 있다.
                 'Open' : sqlalchemy.types.BIGINT(),
                 'High' : sqlalchemy.types.BIGINT(),
                 'Low' : sqlalchemy.types.BIGINT(),
                 'Close' : sqlalchemy.types.BIGINT(),
                 'Volume' : sqlalchemy.types.BIGINT(),
                 'Change' : sqlalchemy.types.FLOAT(),
                 'Code' : sqlalchemy.types.VARCHAR(10),
                 'Name' : sqlalchemy.types.TEXT(),
                 'Date' : sqlalchemy.types.DATE(),
             }
            )
        engine.dispose()

    # 3. 수집한 주식 종목을 기반으로 재무제표 수집 및 DB에 저장 (from FnGuide)

    # 손익계산서 불러오기
    def getIS(self, stock_code, rpt_type, freq):
        items_en = ['rev', 'cgs', 'gross', 'sga', 'sga1', 'sga2', 'sga3', 'sga4', 'sga5', 'sga6', 'sga7', 'sga8', 'opr', 'opr_',
                'fininc', 'fininc1', 'fininc2', 'fininc3', 'fininc4', 'fininc5',
                'fininc6', 'fininc7', 'fininc8', 'fininc9', 'fininc10', 'fininc11',
                'fincost', 'fincost1', 'fincost2', 'fincost3', 'fincost4', 'fincost5',
                'fincost6', 'fincost7', 'fincost8', 'fincost9', 'fincost10',
                'otherrev', 'otherrev1', 'otherrev2', 'otherrev3', 'otherrev4', 'otherrev5', 'otherrev6', 'otherrev7', 'otherrev8',
                'otherrev9', 'otherrev10', 'otherrev11', 'otherrev12', 'otherrev13', 'otherrev14', 'otherrev15', 'otherrev16', 
                'othercost', 'othercost1', 'othercost2', 'othercost3', 'othercost4', 'othercost5',
                'othercost6', 'othercost7', 'othercost8', 'othercost9', 'othercost10', 'othercost11', 'othercost12', 
                'otherpl', 'otherpl1', 'otherpl2', 'otherpl3', 'otherpl4', 'ebit', 'tax', 'contop', 'discontop', 'netinc']
        
        if rpt_type.upper() == 'CONSOLIDATED':
            url = "https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?"+ \
                    "pGB=1&gicode=A{}&cID=&MenuYn=Y&ReportGB=D&NewMenuID=103&stkGb=701".format(stock_code)
            items_en += ['netinc1', 'netinc2']
        
        else:  # 'Unconsolidated'
            url = "https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?"+ \
                    "pGB=1&gicode=A{}&cID=&MenuYn=Y&ReportGB=B&NewMenuID=103&stkGb=701".format(stock_code)
                
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36"
        }
        req = Request(url=url, headers=headers)
        html = urlopen(req).read()
        soup = BeautifulSoup(html, 'html.parser')
        
        if freq.upper() == 'A':
            is_a = soup.find(id = 'divSonikY')
            num_col = 3
        else:  # 'Q'
            is_a = soup.find(id = 'divSonikQ')
            num_col = 4 
        
        if str(type(is_a)) == "<class 'NoneType'>":
            return None
        
        is_a = is_a.find_all(['tr'])
        
        items_kr = [is_a[m].find(['th']).get_text().replace('\n','').replace('\xa0','').replace('계산에 참여한 계정 펼치기','') \
            for m in range(1, len(is_a))]
        
        period = [is_a[0].find_all('th')[n].get_text() for n in range(1, num_col+1)]

        
        for item, i in zip(items_en, range(1, len(is_a))):
            temps = []
            for j in range(0, num_col):
                temp = [float(is_a[i].find_all('td')[j]['title'].replace(',','').replace('\xa0',''))\
                    if is_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '') != '' \
                    else (0 if is_a[i].find_all('td')[j]['title'].replace(',','').replace('\xa0','') == '-0' \
                            else 0)]

                temps.append(temp[0])
                
            globals()[item] = temps
            
        if rpt_type.upper() == 'CONSOLIDATED':
            pass
        else:
            globals()['netinc1'], globals()['netinc2'] = [np.NaN]*num_col, [np.NaN]*num_col
        
        is_domestic = pd.DataFrame({'stock_code':stock_code, 'period':period, 
                                'Revenue':rev, 'Cost_of_Goods_sold':cgs, 'Gross_Profit':gross,
                            'Sales_General_Administrative_Exp_Total': sga, 
                            'Operationg_Profit_Total':opr, 'Operating_Profit_Total_': opr_,
                            'Financial_Income_Total': fininc,
                            'Financial_Costs_Total': fincost,
                            'Other_Income_Total': otherrev,
                            'Other_Costs_Total': othercost,
                            'Subsidiaries_JointVentures_PL_Total': otherpl,
                            'EBIT': ebit, 'Income_Taxes_Exp': tax, 'Profit_Cont_Operation': contop,
                            'Profit_Discont_Operation': discontop, 'Net_Income_Total':netinc,
                            'Net_Income_Controlling': globals()['netinc1'],
                            'Net_Income_Noncontrolling': globals()['netinc2']})
        
        is_domestic = is_domestic.drop(columns=['Operating_Profit_Total_'])    
        is_domestic['rpt_type'] = rpt_type + '_' + freq.upper()
        
        return is_domestic

    # 재무상태표 불러오기
    def getBS(self, stock_code, rpt_type, freq):
        items_en = ['assets', 'curassets', 'curassets1', 'curassets2', 'curassets3', 'curassets4', 'curassets5',
                'curassets6', 'curassets7', 'curassets8', 'curassets9', 'curassets10', 'curassets11',
                'ltassets', 'ltassets1', 'ltassets2', 'ltassets3', 'ltassets4', 'ltassets5', 'ltassets6', 'ltassets7',
                'ltassets8', 'ltassets9', 'ltassets10', 'ltassets11', 'ltassets12', 'ltassets13', 'finassets',
                'liab', 'curliab', 'curliab1', 'curliab2', 'curliab3', 'curliab4', 'curliab5',
                'curliab6', 'curliab7', 'curliab8', 'curliab9', 'curliab10', 'curliab11', 'curliab12', 'curliab13',
                'ltliab', 'ltliab1', 'ltliab2', 'ltliab3', 'ltliab4', 'ltliab5', 'ltliab6', 
                'ltliab7', 'ltliab8', 'ltliab9', 'ltliab10', 'ltliab11', 'ltliab12', 'finliab',
                'equity', 'equity1', 'equity2', 'equity3', 'equity4', 'equity5', 'equity6', 'equity7', 'equity8']

        if rpt_type.upper() == 'CONSOLIDATED':
            url = "https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?"+ \
                    "pGB=1&gicode=A{}&cID=&MenuYn=Y&ReportGB=D&NewMenuID=103&stkGb=701".format(stock_code)
        
        else:  # 'Unconsolidated'
            url = "https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?"+ \
                    "pGB=1&gicode=A{}&cID=&MenuYn=Y&ReportGB=B&NewMenuID=103&stkGb=701".format(stock_code)
            items_en = [item for item in items_en if item not in ['equity1', 'equity8']]
                
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36"
        }
        req = Request(url=url, headers=headers)
        html = urlopen(req).read()
        soup = BeautifulSoup(html, 'html.parser')
        
        if freq.upper() == 'A':
            bs_a = soup.find(id = 'divDaechaY')
            num_col = 3
        else:  # 'Q'
            bs_a = soup.find(id = 'divDaechaQ')
            num_col = 4 
        
        if str(type(bs_a)) == "<class 'NoneType'>":
            return None
        
        bs_a = bs_a.find_all(['tr'])
        
        items_kr = [bs_a[m].find(['th']).get_text().replace('\n','').replace('\xa0','').replace('계산에 참여한 계정 펼치기','') \
            for m in range(1, len(bs_a))]
        
        period = [bs_a[0].find_all('th')[n].get_text() for n in range(1, num_col+1)]

        
        for item, i in zip(items_en, range(1, len(bs_a))):
            temps = []
            for j in range(0, num_col):
                temp = [float(bs_a[i].find_all('td')[j]['title'].replace(',','').replace('\xa0',''))\
                    if bs_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '') != '' \
                    else (0 if bs_a[i].find_all('td')[j]['title'].replace(',','').replace('\xa0','') == '-0' \
                            else 0)]

                temps.append(temp[0])
                
            globals()[item] = temps
            
        if rpt_type.upper() == 'CONSOLIDATED':
            pass
        else:
            globals()['equity1'], globals()['equity8'] = [np.NaN]*num_col, [np.NaN]*num_col
        
        bs_domestic = pd.DataFrame({'stock_code':stock_code, 'period':period, 
                                'Assets_Total':assets, 'Current_Assets_Total':curassets, 'LT_Assets_Total':ltassets,
                            'Liabilities_Total': liab, 'Current_Liab_Total': curliab, 'LT_Liab_Total': ltliab,
                            'Equity_Total':equity, 'Controlling_Equity_Total': equity1, 'Non_Controlling_Equity_Total': equity8})                           
        
        bs_domestic['rpt_type'] = rpt_type + '_' + freq.upper()
        
        return bs_domestic

    # 현금흐름표 불러오기
    def getCF(self, stock_code, rpt_type, freq):
        items_en = ['cfo', 'cfo1', 'cfo2', 'cfo3', 'cfo4', 'cfo5', 'cfo6', 'cfo7',
                'cfi', 'cfi1', 'cfi2', 'cfi3', 'cff', 'cff1', 'cff2', 'cff3',
                'cff4', 'cff5', 'cff6', 'cff7', 'cff8', 'cff9']            

        if rpt_type.upper() == 'CONSOLIDATED':
            url = "https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?"+ \
                    "pGB=1&gicode=A{}&cID=&MenuYn=Y&ReportGB=D&NewMenuID=103&stkGb=701".format(stock_code)
        
        else:  # 'Unconsolidated'
            url = "https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?"+ \
                    "pGB=1&gicode=A{}&cID=&MenuYn=Y&ReportGB=B&NewMenuID=103&stkGb=701".format(stock_code)
                
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36"
        }
        req = Request(url=url, headers=headers)
        html = urlopen(req).read()
        soup = BeautifulSoup(html, 'html.parser')
        
        if freq.upper() == 'A':
            cf_a = soup.find(id = 'divCashY')
            num_col = 3
        else:  # 'Q'
            cf_a = soup.find(id = 'divCashQ')
            num_col = 4 
        
        if str(type(cf_a)) == "<class 'NoneType'>":
            return None

        cf_a = cf_a.find_all(['tr'])
        if len(cf_a) != 159:
            return None

        items_kr = [cf_a[m].find(['th']).get_text().replace('\n','').replace('\xa0','').replace('계산에 참여한 계정 펼치기','') \
            for m in range(1, len(cf_a))]
        
        period = [cf_a[0].find_all('th')[n].get_text() for n in range(1, num_col+1)]

        idx = [1,2,3,4,39,70,75,76,84,85,99,113,121,122,134,145,153,154,155,156,157,158]
        for item, i in zip(items_en, idx):
            temps = []
            for j in range(0, num_col):
                temp = [float(cf_a[i].find_all('td')[j]['title'].replace(',','').replace('\xa0',''))\
                    if cf_a[i].find_all('td')[j]['title'].replace(',', '').replace('\xa0', '') != '' \
                    else (0 if cf_a[i].find_all('td')[j]['title'].replace(',','').replace('\xa0','') == '-0' \
                            else 0)]

                temps.append(temp[0])
                
            globals()[item] = temps
        
        cf_domestic = pd.DataFrame({'stock_code':stock_code, 'period':period, 
                                'CFO_Total':cfo, 'Net_Income_Total':cfo1, 'Cont_Biz_Before_Tax':cfo2,
                            'Add_Exp_WO_CF_Out': cfo3, 'Ded_Rev_WO_CF_In': cfo4, 'Chg_Working_Capital': cfo5,
                            'CFO':cfo6, 'Other_CFO': cfo7,
                                'CFI_Total':cfi, 'CFI_In':cfi1, 'CFI_Out':cfi2, 'Other_CFI':cfi3,
                                'CFF_Total':cff, 'CFF_In':cff1,'CFF_Out':cff2, 'Other_CFF':cff3,
                                'Other_CF':cff4, 'Chg_CF_Consolidation':cff5, 'Forex_Effect':cff6,
                                'Chg_Cash_and_Cash_Equivalents':cff7, 'Cash_and_Cash_Equivalents_Beg':cff8,
                                'Cash_and_Cash_Equivalents_End':cff9})
        
        cf_domestic['rpt_type'] = rpt_type + '_' + freq.upper()
        
        return cf_domestic

    # KRX 상장기업 리스트 수집
    def read_krx_code(self):
        """KRX로부터 상장기업 목록 파일을 읽어와서 데이터프레임으로 반환"""
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method='\
            'download&searchType=13'
        krx = pd.read_html(url, header=0)[0]
        krx = krx[['종목코드']]
        krx = krx.rename(columns={'종목코드': 'code'})
        krx.code = krx.code.map('{:06d}'.format)

        stock_list = np.array(krx.values.tolist()).flatten().tolist()
        return stock_list

    # db에 재무제표 저장
    # fsid: IS(손익계산서), BS(재무상태표), CF(현금흐름표)
    # rpt_type: UNCONSOLIDATED(별도), CONSOLIDATED(연결)
    # freq: A(연간), Q(분기)
    def save_financial_statement(self, data, fsid, rpt_type, freq, server, user, password, db):
        # sqlalchemy의 create_engine을 이용해 DB 연결
        engine = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(user,password,server,db))

        if str(type(data)) != "<class 'NoneType'>":
            data.to_sql(name='krx_'+fsid+'_'+rpt_type +'_'+freq, con=engine,if_exists='append',index=False)

        engine.dispose()

    # 4. db에서 데이터 불러오기

    # 수정주가 데이터 불러오기
    # term: 기간(ex. 2021/1)
    # market: 시장구분(kospi, kosdaq, konex)
    def get_price(self, term, market, server, port, user, password, db):
        # 분기별 시작날까/종료날짜 설정
        if term[5] == '1': # 1분기 (1월~3월)
            start_date = term[0:4] + '-01-01'
            end_date = term[0:4] + '-03-31'
            period = term[0:4] + '/03'

        elif term[5] == '2': # 2분기 (4월~6월)
            start_date = term[0:4] + '-04-01'
            end_date = term[0:4] + '-06-30'
            period = term[0:4] + '/06'

        elif term[5] == '3': # 3분기 (7월~9월)
            start_date = term[0:4] + '-07-01'
            end_date = term[0:4] + '-09-30'
            period = term[0:4] + '/09'

        elif term[5] == '4': # 4분기 (10월~12월)
            start_date = term[0:4] + '-10-01'
            end_date = term[0:4] + '-12-31'
            period = term[0:4] + '/12'

        # db에 연결
        conn = pymysql.connect(host = server, port = port, db = db,
                                user = user, passwd = password, autocommit = True)
        cursor = conn.cursor()

        # stock_code의 start_date와 end_date 데이터 불러오기
        sql = "SELECT * FROM "+market+"_adj WHERE\
            DATE(date) BETWEEN '{}' AND '{}'".format(start_date, end_date)
        
        sql2 = "SELECT * FROM "+market+"_unadj WHERE\
            DATE(date) BETWEEN '{}' AND '{}'".format(end_date, end_date)
        
        cursor.execute(sql)

        df = pd.DataFrame(cursor.fetchall())
        df.columns = [col[0] for col in cursor.description]
        df['Date'] = df['Date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        
        cursor.execute(sql2)

        df2 = pd.DataFrame(cursor.fetchall())
        df2.columns = [col[0] for col in cursor.description]
        df2['date'] = df2['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        
        cursor.close()
        conn.close()

        print('start_date({}) ~ end_date({})'.format(start_date, end_date))
        
        df = df.sort_values(by=['Code', 'Date'], axis=0) # stock_code, date별로 정렬
        
        df2 = df2.iloc[:,[0,1,2,-1,-2]]
        df2 = df2.sort_values(by=['stock_code', 'date'], axis=0) # stock_code, date별로 정렬
        
        # 결측치 처리
        df[['Open', 'High', 'Low', 'Close']] = df[['Open', 'High', 'Low', 'Close']].replace(0, np.nan)

        df['Open'] = np.where(pd.notnull(df['Open']) == True, df['Open'], df['Close'])
        df['High'] = np.where(pd.notnull(df['High']) == True, df['High'], df['Close'])
        df['Low'] = np.where(pd.notnull(df['Low']) == True, df['Low'], df['Close'])
        df['Close'] = np.where(pd.notnull(df['Close']) == True, df['Close'], df['Close'])

        # stock_code 별로 통계 모으기    `
        groups = df.groupby('Code')

        df_ohlc = pd.DataFrame()
        df_ohlc['high'] = groups.max()['High'] # 분기별 고가
        df_ohlc['low'] = groups.min()['Low'] # 분기별 저가
        df_ohlc['period'] = period # 분기 이름 설정
        df_ohlc['open'], df_ohlc['close'], df_ohlc['volume'] = np.nan, np.nan, np.nan
        df_ohlc['시가총액'], df_ohlc['상장주식수'] = np.nan, np.nan

        df_ohlc['stock_code'] = df_ohlc.index 
        df_ohlc = df_ohlc.reset_index(drop=True)

        for i in range(len(df_ohlc)):
            df_ohlc['open'][i] = float(df[df['Code']==df_ohlc['stock_code'][i]].head(1)['Open']) # 분기별 시가
            df_ohlc['close'][i] = float(df[df['Code']==df_ohlc['stock_code'][i]].tail(1)['Close']) # 분기별 저가
            df_ohlc['volume'][i] = float(df[df['Code']==df_ohlc['stock_code'][i]].tail(1)['Volume']) # 분기별 거래량
            if not df2[df2['stock_code']==df_ohlc['stock_code'][i]].tail(1).empty:
                df_ohlc['시가총액'][i] = float(df2[df2['stock_code']==df_ohlc['stock_code'][i]].tail(1)['MKTCAP']) # 분기별 시가총액
                df_ohlc['상장주식수'][i] = float(df2[df2['stock_code']==df_ohlc['stock_code'][i]].tail(1)['LIST_SHRS']) # 분기별 상장주식수
            
        df_ohlc = df_ohlc[['stock_code', 'period', 'open', 'high', 'low', 'close', 'volume', '시가총액', '상장주식수']]
        return df_ohlc, period

    # 종목별 주가데이터 불러오기
    # term: 기간(ex. 2021/1)
    # market: 시장구분(kospi, kosdaq, konex)
    def get_price_backtest(self, stock_code, term, market, server, port, user, password, db):
        # 분기별 시작날까/종료날짜 설정
        if term[5] == '1': # 1분기 (작년 4월~3월)
            start_date = str(int(term[0:4])-1) + '-04-01'
            end_date = term[0:4] + '-03-31'

        elif term[5] == '2': # 2분기 (작년 7월~6월)
            start_date = str(int(term[0:4])-1) + '-07-01'
            end_date = term[0:4] + '-06-30'

        elif term[5] == '3': # 3분기 (작년 10월~9월)
            start_date = str(int(term[0:4])-1) + '-09-01'
            end_date = term[0:4] + '-09-30'

        elif term[5] == '4': # 4분기 (1월~12월)
            start_date = term[0:4] + '-01-01'
            end_date = term[0:4] + '-12-31'

        # db에 연결
        conn = pymysql.connect(host = server, port = port, db = db,
                                    user = user, passwd = password, autocommit = True)
        cursor = conn.cursor()

        # stock_code의 start_date와 end_date 데이터 불러오기
        sql = "SELECT * FROM "+market+"_adj WHERE Code='{}' AND DATE(Date) BETWEEN '{}' AND '{}'".format(stock_code, start_date, end_date)
        
        cursor.execute(sql)

        df = pd.DataFrame(cursor.fetchall())
        df.columns = [col[0] for col in cursor.description]
        df['Date'] = df['Date'].apply(lambda x: x.strftime('%Y-%m-%d'))

        cursor.close()
        conn.close()
        
        df = df[['Code', 'Date', 'Close']] # 종가만 갖고오기
        
        return df
    
    # db에서 재무데이터 불러오기

    # 포괄손익계산서
    def get_is_from_db(self, stock_code, period, server, port, user, password, db):
        # db에 연결
        conn = pymysql.connect(host = server, port = port, db = db,
                                user = user, passwd = password, autocommit = True)
        cursor = conn.cursor()

        # stock_code의 start_date와 end_date 데이터 불러오기
        sql = "SELECT * FROM krx_is_consolidated_q WHERE stock_code='{}' AND period='{}' AND rpt_type='CONSOLIDATED_Q'".format(stock_code, period)

        cursor.execute(sql)

        df_is = pd.DataFrame(cursor.fetchall())
        if len(df_is.columns) != len([col[0] for col in cursor.description]):
            df_is = pd.DataFrame(columns=['stock_code','period','rpt_type','매출액','매출원가', '매출총이익', '판매비와 관리비', 
                    '영업이익', '금융이익', '금융원가', '기타수익', '기타비용', '종속기업,공동지배기업및관계기업관련손익', 
                    '세전계속사업이익', '법인세비용', '계속영업이익', '중단영업이익', 
                    '당기순이익'])
            df_is = df_is.append(pd.Series(), ignore_index=True)
            cursor.close()
            conn.close()
            return df_is

        df_is.columns = [col[0] for col in cursor.description]

        cursor.close()
        conn.close()
        
        df_is = df_is.rename(columns={
            'Revenue':'매출액',
            'Cost_of_Goods_sold':'매출원가',
            'Gross_Profit':'매출총이익',
            'Sales_General_Administrative_Exp_Total':'판매비와 관리비',
            'Operating_Profit_Total':'영업이익',
            'Financial_Income_Total':'금융이익',
            'Financial_Costs_Total':'금융원가',
            'Other_Income_Total':'기타수익',
            'Other_Costs_Total':'기타비용',
            'Subsidiaries_JointVentures_PL_Total':'종속기업,공동지배기업및관계기업관련손익',
            'EBIT':'세전계속사업이익',
            'Income_Taxes_Exp':'법인세비용',
            'Profit_Cont_Operation':'계속영업이익',
            'Profit_Discont_Operation':'중단영업이익',
            'Net_Income_Total':'당기순이익'
        })
        
        df_is = df_is[['stock_code', 'period', 'rpt_type', 
                    '매출액', '매출원가', '매출총이익', '판매비와 관리비', 
                    '영업이익', '금융이익', '금융원가', '기타수익', '기타비용', '종속기업,공동지배기업및관계기업관련손익', 
                    '세전계속사업이익', '법인세비용', '계속영업이익', '중단영업이익', 
                    '당기순이익']]
        
        return df_is

    # 재무상태표
    def get_bs_from_db(self, stock_code, period, server, port, user, password, db):
        # db에 연결
        conn = pymysql.connect(host = server, port = port, db = db,
                                user = user, passwd = password, autocommit = True)
        cursor = conn.cursor()

        # stock_code의 start_date와 end_date 데이터 불러오기
        sql = "SELECT * FROM krx_bs_consolidated_q WHERE stock_code='{}' AND period='{}' AND rpt_type='CONSOLIDATED_Q'".format(stock_code, period)

        cursor.execute(sql)

        df_bs = pd.DataFrame(cursor.fetchall())

        if len(df_bs.columns) != len([col[0] for col in cursor.description]):
            df_bs = pd.DataFrame(columns=['stock_code', 'period', 'rpt_type', 
                    '자산', '유동자산', '비유동자산',
                    '부채', '유동부채', '비유동부채', 
                    '자본'])
            df_bs = df_bs.append(pd.Series(), ignore_index=True)
            cursor.close()
            conn.close()
            return df_bs

        df_bs.columns = [col[0] for col in cursor.description]

        cursor.close()
        conn.close()
        
        df_bs = df_bs.rename(columns={
            'Assets_Total':'자산',
            'Current_Assets_Total':'유동자산',
            'LT_Assets_Total':'비유동자산',
            'Liabilities_Total':'부채',
            'Current_Liab_Total':'유동부채',
            'LT_Liab_Total':'비유동부채',
            'Equity_Total':'자본',
        })

        df_bs = df_bs[['stock_code', 'period', 'rpt_type', 
                    '자산', '유동자산', '비유동자산',
                    '부채', '유동부채', '비유동부채', 
                    '자본']]
        
        return df_bs
    
    # 현금흐름표
    def get_cf_from_db(self, stock_code, period, server, port, user, password, db):
        # db에 연결
        conn = pymysql.connect(host = server, port = port, db = db,
                                    user = user, passwd = password, autocommit = True)
        cursor = conn.cursor()

        # stock_code의 start_date와 end_date 데이터 불러오기
        sql = "SELECT * FROM krx_cf_consolidated_q WHERE stock_code='{}' AND period='{}' AND rpt_type='CONSOLIDATED_Q'".format(stock_code, period)

        cursor.execute(sql)

        df_cf = pd.DataFrame(cursor.fetchall())
        df_cf.columns = [col[0] for col in cursor.description]

        cursor.close()
        conn.close()
        
        df_cf = df_cf.rename(columns={
            'CFO_Total':'영업활동으로인한현금흐름',
            'Net_Income_Total':'당기순손익',
            'Cont_Biz_Before_Tax':'법인세비용차감전계속사업이익',
            'Add_Exp_WO_CF_Out':'현금유출이없는비용등가산',
            'Ded_Rev_WO_CF_In':'(현금유입이없는수익등차감)',
            'Chg_Working_Capital':'영업활동으로인한자산부채변동(운전자본변동)',
            'CFO':'*영업에서창출된현금흐름', 'Other_CFO':'기타영업활동으로인한현금흐름',
            'CFI_Total':'투자활동으로인한현금흐름',
            'CFI_In':'투자활동으로인한현금유입액',
            'CFI_Out':'(투자활동으로인한현금유출액)',
            'Other_CFI':'기타투자활동으로인한현금흐름',
            'CFF_Total':'재무활동으로인한현금흐름', 
            'CFF_In':'재무활동으로인한현금유입액', 
            'CFF_Out':'(재무활동으로인한현금유출액)',
            'Other_CFF':'기타재무활동으로인한현금흐름',
            'Other_CF':'영업투자재무활동기타현금흐름', 
            'Chg_CF_Consolidation':'연결범위변동으로인한현금의증가',
            'Forex_Effect':'환율변동효과',
            'Chg_Cash_and_Cash_Equivalents':'현금및현금성자산의증가', 
            'Cash_and_Cash_Equivalents_Beg':'기초현금및현금성자산',
            'Cash_and_Cash_Equivalents_End':'기말현금및현금성자산'
        })

        df_cf = df_cf[['stock_code', 'period', 'rpt_type', 
                    
                    '영업활동으로인한현금흐름', '당기순손익', '법인세비용차감전계속사업이익', '현금유출이없는비용등가산',
                    '(현금유입이없는수익등차감)', '영업활동으로인한자산부채변동(운전자본변동)', 
                    '*영업에서창출된현금흐름', '기타영업활동으로인한현금흐름',
                    
                    '투자활동으로인한현금흐름', '투자활동으로인한현금유입액', 
                    '(투자활동으로인한현금유출액)', '기타투자활동으로인한현금흐름',
                    
                    '재무활동으로인한현금흐름', '재무활동으로인한현금유입액', 
                    '(재무활동으로인한현금유출액)', '기타재무활동으로인한현금흐름',
                    
                    '영업투자재무활동기타현금흐름', '연결범위변동으로인한현금의증가', '환율변동효과', 
                    '현금및현금성자산의증가', '기초현금및현금성자산', '기말현금및현금성자산']]
        
        return df_cf

    # Trailing 데이터 생성
    def get_trailing(self, stock_code, period, server, port, user, password, db):
        df_quat = {}
        period_quat = period

        for i in range(4):
            print(period_quat, end=" ")
            df_is = self.get_is_from_db(stock_code, period_quat, server, port, user, password, db) # 포괄손익계산서
            df_bs = self.get_bs_from_db(stock_code, period_quat, server, port, user, password, db) # 재무상태표
            df_cf = self.get_cf_from_db(stock_code, period_quat, server, port, user, password, db) # 현금흐름표

            df_merge = pd.merge(df_is, df_bs, how='left', on=['stock_code', 'period', 'rpt_type']) # 
            df_merge = pd.merge(df_merge, df_cf, how='left', on=['stock_code', 'period', 'rpt_type'])

            df_quat[i] = df_merge

            if int(period_quat[5:7])-3 > 0:
                period_quat = period_quat[0:4] + '/' + str(int(period_quat[5:7])-3).zfill(2)

            else:
                period_quat = str(int(period_quat[0:4])-1) + '/12'
            
        df_trailing = pd.DataFrame(df_quat[0], columns=['stock_code', 'period', 'rpt_type'])
        df_trailing[df_quat[0].columns[3:]] = 0

        for i in range(len(df_quat)):
            df_trailing[df_quat[0].columns[3:]] += df_quat[i][df_quat[i].columns[3:]]
            
        return df_trailing

    # 5. 종목 찾기

    # 종목 찾기
    #MKTCAP_top: 시가총액 상위 % (ex. 0.2)
    #factor_list: 팩터 리스트 (ex. ['PER', 'PBR'])
    #n: 상위 n개 종목 (ex. 30)
    def stock_select(self, df_factor, MKTCAP_top, n, factor_list):
        basic_list = ['stock_code', 'period', '시가총액']
        basic_list.extend(factor_list)

        df_select = df_factor.copy()
        df_select = df_select[basic_list]

        df_select['score'] = 0

        # 시가총액 상위 MKTCAP_top(%) 산출
        df_select = df_select.sort_values(by=['시가총액'], ascending=False).head(int(len(df_select) * MKTCAP_top))
        df_select = df_select.dropna()

        # 팩터간의 점수 계산
        for i in range(len(factor_list)):
            df_select[factor_list[i] + '_score'] = (df_select[factor_list[i]] - max(df_select[factor_list[i]]))
            df_select[factor_list[i] + '_score'] = df_select[factor_list[i] + '_score']/min(df_select[factor_list[i] + '_score'])

            df_select['score'] += (df_select[factor_list[i] + '_score'] / len(factor_list))

        # 상위 n개 종목 추출
        df_select = df_select.sort_values(by=['score'], ascending=False).head(n)

        # 종목 선택
        stock_select = list(df_select['stock_code'])
        
        return stock_select
    
    # per값 계산
    def getPER(self, df_factor, term, server, port, user, password, db):
        stock_list = df_factor['stock_code'].to_list()

        # 당기순이익을 가져오기 위해 is데이터 불러오기
        df_mrg = pd.DataFrame()
        for stock in tqdm(stock_list):
            is_data = self.get_is_from_db(stock, term, server, port, user, password, db)
            df_mrg = pd.concat([df_mrg, is_data])

        df_mrg = df_mrg.reset_index(drop=True)

        # 당기순이익 추가
        df_factor = pd.concat([df_factor, df_mrg['당기순이익']], axis = 1)

        # EPS 계산
        fin_unit = 100000000
        df_factor['EPS'] = (df_factor['당기순이익']* fin_unit)/df_factor['상장주식수']
        
        # PER 계산
        per = []
        for i in df_factor.index:
            if df_factor.loc[i,'EPS'] != 0:
                per.append(df_factor.loc[i,'close'] / df_factor.loc[i,'EPS'])
            else:
                per.append(np.nan)

        df_factor['PER'] = per

        return df_factor

    # pbr값 계산
    def getPBR(self, df_factor, term, server, port, user, password, db):
        stock_list = df_factor['stock_code'].to_list()

        # 자본을 가져오기 위해 bs데이터 불러오기
        df_mrg = pd.DataFrame()
        for stock in tqdm(stock_list):
            bs_data = self.get_bs_from_db(stock, term, server, port, user, password, db)
            df_mrg = pd.concat([df_mrg, bs_data])

        df_mrg = df_mrg.reset_index(drop=True)

        # 자본 추가
        df_factor = pd.concat([df_factor, df_mrg['자본']], axis = 1)

        # BPS 계산
        fin_unit = 100000000
        df_factor['BPS'] = (df_factor['자본']*fin_unit)/df_factor['상장주식수']
        
        # PBR 계산
        pbr = []
        for i in df_factor.index:
            if df_factor.loc[i,'BPS'] != 0:
                pbr.append(df_factor.loc[i,'close'] / df_factor.loc[i,'BPS'])
            else:
                pbr.append(np.nan)

        df_factor['PBR'] = pbr

        return df_factor


kse = krx_stock_extraction()