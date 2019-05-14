import urllib.parse
import psycopg2
import time 
import DongANews as dan
import ChosunNews as chn
import JKSL_paper as jkp
import CNN_news as cnn
import joongangnews as jon
import datetime
import re
#데이터 수집 뉴스 키워드 정보 호출
def clkwSelectTblNewsqQery():
    try:
            keywordQueryList=[]
            #   Default port =1200
            conn_string = "host='121.160.17.80' dbname ='EcoBank' user='dev' password='nie12345' port='12000'"
            conn = psycopg2.connect(conn_string)
            cur = conn.cursor()


            keyword_query="""SELECT master.data_clkw_no   AS "pknumber" 
                                       , master.data_colct_trget_se_code AS "tgetCode" 
                                       , master.data_clkw_ttle           AS "keyword" 
                                       , master.data_colct_opert_at      AS "opertAt"                                      
                                FROM   data_scraping_analysis.tbl_data_clkw AS master                                      
                                WHERE  master.data_colct_trget_se_code = '일간지' 
                                         AND master.data_colct_opert_at = 'Y'"""


            cur.execute(keyword_query)
            rows = cur.fetchall()
            for row in rows:
                keywordQueryList.append(row)
            conn.close()
            return keywordQueryList  
    except  (Exception, psycopg2.DatabaseError) as error:
        print(error)
        print ('postgresql database connection error!')

    finally:
        if conn is not None:
            conn.close()
#데이터 수집 논문 키워드 정보 호출
def clkwSelectTblThssqQery():
    try:
            keywordQueryList=[]
            #   Default port =1200
            conn_string = "host='121.160.17.80' dbname ='EcoBank' user='dev' password='nie12345' port='12000'"
            conn = psycopg2.connect(conn_string)
            cur = conn.cursor()

            keyword_query="""SELECT master.data_clkw_no   AS "pknumber" 
                                   , master.data_colct_trget_se_code AS "tgetCode" 
                                   , master.data_clkw_ttle           AS "keyword" 
                                   , master.data_colct_opert_at      AS "opertAt" 

                            FROM   data_scraping_analysis.tbl_data_clkw AS master 

                            WHERE  master.data_colct_trget_se_code = '논문' 
                                   AND master.data_colct_opert_at = 'Y'"""
            cur.execute(keyword_query)
            rows = cur.fetchall()
            for row in rows:
                keywordQueryList.append(row)
            conn.close()
            return keywordQueryList  
    except  (Exception, psycopg2.DatabaseError) as error:
        print(error)
        print ('postgresql database connection error!')

    finally:
        if conn is not None:
            conn.close()
# 데이터 수집 논문 코드정보 호출
def clorsSlectTblThssQery():
    try:
            keyword_query=[]
            #   Default port =1200
            conn_string = "host='121.160.17.80' dbname ='EcoBank' user='dev' password='nie12345' port='12000'"
            conn = psycopg2.connect(conn_string)
            cur = conn.cursor()


            taget_query ="""SELECT data_clor_no 
                                , data_colct_trget_se_code 
                                , data_clor_ttle 
                                FROM data_scraping_analysis.tbl_data_clor
                                WHERE data_colct_trget_se_code='논문'
                                    """ 
            cur.execute(taget_query)
            rows = cur.fetchall()
            for row in rows:
                keyword_query.append(row)
            conn.close()
            return keyword_query  
    except  (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()
#데이터 수집 뉴스 코드 정보 호출
def clorsSlectTblNewsQery():
    try:
            keyword_query=[]
            #   Default port =1200
            conn_string = "host='121.160.17.80' dbname ='EcoBank' user='dev' password='nie12345' port='12000'"
            conn = psycopg2.connect(conn_string)
            cur = conn.cursor()


            taget_query ="""SELECT data_clor_no 
                                , data_colct_trget_se_code 
                                , data_clor_ttle 
                                FROM data_scraping_analysis.tbl_data_clor
                                WHERE data_colct_trget_se_code='일간지'
                                """   
            cur.execute(taget_query)
            rows = cur.fetchall()
            for row in rows:
                keyword_query.append(row)
            conn.close()
            return keyword_query  
    except  (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()

def tblDataCstQery():
    try:
            keyword_query=[]
            #   Default port =1200
            conn_string = "host='121.160.17.80' dbname ='EcoBank' user='dev' password='nie12345' port='12000'"
            conn = psycopg2.connect(conn_string)
            cur = conn.cursor()


            taget_query ="""
                    SELECT data_clst_no 
                           , data_clst_kwrd_no 
                           , data_clst_orig_no 
                           , data_colct_url 
                           , data_pge_no 
                           , data_list_no 
                    FROM   data_scraping_analysis.tbl_data_clst      
                                """   
            cur.execute(taget_query)
            rows = cur.fetchall()
            for row in rows:
                keyword_query.append(row)
            conn.close()
            return keyword_query  
        
        
        
    except  (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def tblThssDataCstQery():
    try:
            keyword_query=[]
            #   Default port =1200
            conn_string = "host='121.160.17.80' dbname ='EcoBank' user='dev' password='nie12345' port='12000'"
            conn = psycopg2.connect(conn_string)
            cur = conn.cursor()


            taget_query ="""
                    SELECT data_clst_no 
                           , data_clst_kwrd_no 
                           , data_clst_orig_no 
                           , data_colct_url 
                           , data_pge_no 
                           , data_list_no 
                        FROM   data_scraping_analysis.tbl_data_clst
                        where data_clst_orig_no=1
                                """   
            cur.execute(taget_query)
            rows = cur.fetchall()
            for row in rows:
                keyword_query.append(row)
            conn.close()
            return keyword_query  
             
        
    except  (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
# clst insrt 쿼리 문
def insertDataClst(InsertTblDataClstData):
    try:
        conn_string = "host='121.160.17.80' dbname ='EcoBank' user='dev' password='nie12345' port='12000'"
        conn = psycopg2.connect(conn_string)
        curs = conn.cursor()   
        insert_keyword_query="""INSERT INTO data_scraping_analysis.tbl_data_clst 
                                    ( 
                                     data_clst_kwrd_no 
                                     , data_clst_orig_no 
                                     , data_colct_url 
                                     , data_pge_no 
                                     , data_list_no 
                                     ) 
                                    VALUES      ( 
                                             %s 
                                             , %s 
                                             , %s 
                                             , %s 
                                             , %s 
                                                );   """
        curs.executemany(insert_keyword_query,InsertTblDataClstData)
        conn.commit()
    except  (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()
#뉴스 수집 insrt 쿼리 문
def insertNewsColct(InsertTblNewsColctData):
    # DB conccert 
    try:
        now = datetime.datetime.now()
        keyword_query=[]
        
    #   Default port =1200
        conn_string = "host='121.160.17.80' dbname ='EcoBank' user='dev' password='nie12345' port='12000'"
        conn = psycopg2.connect(conn_string)
        curs = conn.cursor()   
        insert_keyword_query="""INSERT INTO data_scraping_analysis.tbl_news_colct 
                                    ( 
                                     news_clst_no 
                                     , news_pblc_de 
                                     , news_sj 
                                     , news_bdt 
                                     , news_colct_url 
                                     , news_colct_de) 
                        VALUES      ( 
                                       %s 
                                     , %s 
                                     , %s 
                                     , %s 
                                     , %s 
                                     , To_char(current_date,'YYYYMMDD')); """
        # Execute a statement
        curs.executemany(insert_keyword_query,InsertTblNewsColctData)
        conn.commit()
        print("success")
        
    except  (Exception, psycopg2.DatabaseError) as error:
        print(error)
        print ('postgresql database connection error!')
        
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
#논문 수집 insrt 쿼리문
def insertThssColct(InsertTblThssColctData):
    # DB conccert 
    try:
        now = datetime.datetime.now()
        keyword_query=[]
        
    #   Default port =1200
        conn_string = "host='121.160.17.80' dbname ='EcoBank' user='dev' password='nie12345' port='12000'"
        conn = psycopg2.connect(conn_string)
        curs = conn.cursor()   
        insert_keyword_query="""INSERT INTO data_scraping_analysis.tbl_thss_colct 
                                    ( 
                                       thss_clst_no 
                                     , thss_psitn_lrsp_no 
                                     , thss_pblc_de 
                                     , thss_sj 
                                     , thss_abstr 
                                     , thss_athr_nm 
                                     , thss_pdf_dwld_at 
                                     , thss_colct_url 
                                     , thss_colct_de) 
                        VALUES      ( 
                                       %s 
                                     , %s 
                                     , %s 
                                     , %s 
                                     , %s 
                                     , %s 
                                     , %s 
                                     , %s 
                                     , To_char(current_date,'YYYYMMDD'))    """
        # Execute a statement
        curs.executemany(insert_keyword_query,InsertTblThssColctData)
        conn.commit()
        print("success")
        
    except  (Exception, psycopg2.DatabaseError) as error:
        print(error)
        print ('postgresql database connection error!')
        
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
#메인 함수 호출부
def main(browser_path,output,order_input,order,delay):
    #   program start time
    now = datetime.datetime.now()
    startDate = now.strftime('%Y%m%d')

    # selectQuery
    newsClkw=clkwSelectTblNewsqQery()
    thssClkw=clkwSelectTblThssqQery()

    newsClor=clorsSlectTblNewsQery()
    thssClor=clorsSlectTblThssQery()

    start_time = time.time()



    # dataColctdate
    browser_path='C:/Users/seo/Desktop/phantomjs-2.1.1-windows/bin/phantomjs.exe'
    output='C:/Users/seo/Desktop/BioWebCollect/'

    now = datetime.datetime.now()
    startDate = now.strftime('%Y%m%d')


    dongaClwInsertTblDataClstData=[]
    cnnClwInsertTblDataClstData=[]
    jongClwClwInsertTblDataClstData=[]
    chnClwInsertTblDataClstData=[]

    cnnInsertTblNewsColctData=[]
    dongaInsertTblNewsColctData=[]
    joungInsertTblNewsColctData=[]
    chnInsertTblNewsColctData=[]

    InsertTblNewsColctData=[]
    

    try:
        pageStart=order_input*15
        pageMax=order*15+1
        for pknumber,tgetCode,keyword,opertAt in newsClkw:            
            dongaClw=dan.dongamagazine(pknumber,keyword,browser_path,pageStart,pageMax,delay)
            dongaClwInsertTblDataClstData.append((pknumber,'5',dongaClw[5],dongaClw[6],dongaClw[4]))
            insertDataClst(dongaClwInsertTblDataClstData)
            for data_clst_no, data_clst_kwrd_no, data_clst_orig_no, data_colct_url, data_pge_no, data_list_no  in tblDataCstQery():
                if data_colct_url==dongaClw[5]:
                    checkData=0
                    for checkData in range (checkData ,len (dongaClw[0])-1):        
                        date=dongaClw[3][checkData]
                        regex=re.compile(r'\d\d\d\d-\d\d-\d\d')
                        tt=regex.findall(str(date))
                        dongaDate=re.sub('-','',str(tt))
                        dongaDate=dongaDate.replace("'",'')
                        dongaDate=dongaDate.replace('[','')
                        dongaDate=dongaDate.replace(']','')

                        donganews_sj=dongaClw[0][checkData]
                        donganews_sj=donganews_sj[0:230]

                        donganews_bdt=dongaClw[1][checkData]
                        donganews_bdt=donganews_bdt[0:9800]

                        donganews_colct_url=dongaClw[2][checkData]
                        donganews_colct_url=donganews_colct_url[0:2000]

                        dongaInsertTblNewsColctData.append((data_clst_no,dongaDate,donganews_sj,donganews_bdt,donganews_colct_url))
        insertNewsColct(dongaInsertTblNewsColctData)
    except:
        pass
                    
                        
                        
                        
    try:
        for pknumber,tgetCode,keyword,opertAt in newsClkw:
            cnnClw=cnn.cnn_urlcrawler(pknumber,keyword,browser_path,order_input,order,delay)
            cnnClwInsertTblDataClstData.append((pknumber,'2',cnnClw[5],cnnClw[6],cnnClw[4]))
            insertDataClst(cnnClwInsertTblDataClstData)
            for data_clst_no, data_clst_kwrd_no, data_clst_orig_no, data_colct_url, data_pge_no, data_list_no  in tblDataCstQery():
                if data_colct_url==cnnClw[5]:
                    checkData=0
                    for checkData in range (checkData ,len (cnnClw[0])-1):   
                        cnndate=cnnClw[3][checkData]
                        cnndate=str(cnndate).replace("'",'')
                        cnndate=dongaDate.replace('[','')
                        cnndate=dongaDate.replace(']','')

                        cnnnews_sj=cnnClw[0][checkData]
                        cnnnews_sj=cnnnews_sj[0:230]

                        cnnnews_bdt=cnnClw[1][checkData]
                        cnnnews_bdt=cnnnews_bdt[0:9800]

                        cnnnews_colct_url=cnnClw[2][checkData]
                        cnnnews_colct_url=cnnnews_colct_url[0:2000]
                        cnnInsertTblNewsColctData.append((data_clst_no,cnndate,cnnnews_sj,cnnnews_bdt,cnnClw[2][checkData]))
        insertNewsColct(cnnInsertTblNewsColctData)

    except:
        pass

    try:
        for pknumber,tgetCode,keyword,opertAt in newsClkw:
            jongClw=jon.centermagazine(pknumber,keyword,browser_path,order_input,order,delay)
            jongClwClwInsertTblDataClstData.append((pknumber,'4',jongClw[5],jongClw[6],jongClw[4]))
            insertDataClst(jongClwClwInsertTblDataClstData)
            for data_clst_no, data_clst_kwrd_no, data_clst_orig_no, data_colct_url, data_pge_no, data_list_no  in tblDataCstQery():
                if data_colct_url==jongClw[5]:
                    checkData=0
                    for checkData in range (checkData ,len (jongClw[0])-1):   
                        jdate=jongClw[3][checkData]
                        regex=re.compile(r'\d\d\d\d.\d\d.\d\d')
                        jongtt=regex.findall(str(jdate))
                        JongDate=str(jongtt).replace('.','') 
                        JongDate=JongDate.replace("'",'')
                        JongDate=JongDate.replace('[','')
                        JongDate=JongDate.replace(']','')


                        jongnews_sj=jongClw[0][checkData]
                        jongnews_sj=jongnews_sj[0:230]

                        jongnews_bdt=jongClw[1][checkData]
                        jongnews_bdt=jongnews_bdt[0:9800]

                        jongnews_colct_url=jongClw[2][checkData]
                        jongnews_colct_url=jongnews_colct_url[0:2000]

                        joungInsertTblNewsColctData.append((data_clst_no,JongDate,jongnews_sj,jongnews_bdt,jongnews_colct_url))
        insertNewsColct(joungInsertTblNewsColctData)
                    
    except:
        pass
    
    try:
        for pknumber,tgetCode,keyword,opertAt in newsClkw:
            chnClw=chn.chosunmagazine(pknumber,keyword,browser_path,order_input,order,delay)
            chnClwInsertTblDataClstData.append((pknumber,'3',chnClw[5],chnClw[6],chnClw[4]))
            insertDataClst(chnClwInsertTblDataClstData)
            for data_clst_no, data_clst_kwrd_no, data_clst_orig_no, data_colct_url, data_pge_no, data_list_no  in tblDataCstQery():
                if data_colct_url==chnClw[5]:
                    checkData=0
                    for checkData in range (checkData ,len (chnClw[0])-1): 
                        Cdate=chnClw[3][checkData]
                        Cdate=str(Cdate)
                        Cdate=Cdate.replace('.','')
                        Cdate=re.sub('[가-힝()]','',Cdate) 
                        Cdate=Cdate.replace("'",'')
                        Cdate=Cdate.replace(' ','')



                        chnnews_sj=chnClw[0][checkData]
                        chnnews_sj=chnnews_sj[0:230]

                        chnnews_bdt=chnClw[1][checkData]
                        chnnews_bdt=chnnews_bdt[0:9800]

                        chnnews_colct_url=chnClw[2][checkData]
                        chnnews_colct_url=chnnews_colct_url[0:2000]


                        chnInsertTblNewsColctData.append((data_clst_no,Cdate,chnnews_sj,chnnews_bdt,chnnews_colct_url))
        insertNewsColct(chnInsertTblNewsColctData)
    except:
        pass
    

    try:
        jkpclwClwInsertTblDataClstData=[]
        InsertTblThssColctData=[]

        for pknumber,tgetCode,keyword,opertAt in thssClkw:
            jkpclw=jkp.paper(pknumber,keyword,browser_path,output,order_input,order,delay)
            jkpclwClwInsertTblDataClstData.append((pknumber,'1',jkpclw[7],jkpclw[6],jkpclw[8]))
            insertDataClst(jkpclwClwInsertTblDataClstData)

            for data_clst_no, data_clst_kwrd_no, data_clst_orig_no, data_colct_url, data_pge_no, data_list_no  in tblThssDataCstQery():
                if data_colct_url==jkpclw[7]:
                    checkData=0
                    for checkData in range (checkData ,len(jkpclw[0])): 
                        print()

                        jkpclwdate =jkpclw[3][checkData]
                        jkpclwdate=jkpclwdate[0:4]                    

                        thss_sj = jkpclw[0][checkData]
                        thss_sj=thss_sj[0:250]

                        thss_abstr =jkpclw[1][checkData]
                        thss_abstr=thss_abstr[0:3800]

                        thss_athr_nm =jkpclw[4][checkData]
                        thss_athr_nm=thss_athr_nm[0:90]

                        thss_colct_url=jkpclw[2][checkData]
                        thss_colct_url=thss_colct_url[0:2000]

                        InsertTblThssColctData.append((data_clst_no,'1',jkpclwdate,thss_sj,thss_abstr,thss_athr_nm,jkpclw[5][checkData],thss_colct_url))

            insertThssColct(InsertTblThssColctData)
    except:
        pass

#browser_path= 팬텀 js가 설치된 경로,output= 논문 파일이 다운로드 될 경로, delay=2 페이지 로딩 시간, order_input=1 수집 될 첫 페이지, order=7 수집 종료 페이지
if __name__ == "__main__":
    browser_path='/home/pgmaster/DataCollect/phantom/phantomjs-2.1.1-linux-x86_64/bin/phantomjs.exe'
    output='/home/pgmaster/DataCollect/ThissDownloadPDF/'
    delay=2
    order_input=2
    order=7

    main(browser_path,output,order_input,order,delay)



