from selenium import webdriver
import time
from bs4 import BeautifulSoup
import DatacolctVo as dv
import psycopg2

# CLST 테이블 에서 수집 된 URL 정보 호출
def tblDataCstQery():
    try:
            keyword_query=[]
            #   Default port =1200
            conn_string = "host='121.160.17.80' dbname ='EcoBank' user='dev' password='nie12345' port='12000'"
            conn = psycopg2.connect(conn_string)
            cur = conn.cursor()


            taget_query ="""
                    SELECT data_colct_url 
                    FROM   data_scraping_analysis.tbl_data_clst 
                    Where  data_clst_orig_no = '2'      
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
# URL 중복 체크                
def urlOverlap(url):
    urlData=tblDataCstQery()
    if not urlData== url:
        return url
#목록 파싱
def content(st_list_count,html):
    bodylist=[]
    s1 = html.find_all("div", class_="cnn-search__results")
    body = s1[0].find_all("div", class_="cnn-search__result-body")    
    for b in body:
            b=b.text.strip()
            bodylist.append(b)

    return bodylist[st_list_count-1]
# 제목 파싱
def titlemethod(st_list_count,html):
    titlelist=[]
    linklist=[]
    articlelist=[]
    s1 = html.find_all("div", class_="cnn-search__results")
    s2 = s1[0].find_all("a")

    idx = 0

    for i in s2:
        if idx % 2 == 1:
            articlelist.append(i)
        idx += 1

    order=0
    idx = 0
    listing = order * 10

    for i in articlelist:
        titlelist.append(i.text.strip())
        linklist.append("https:"+str(i).split("href=\"")[1].split("\">")[0])
        idx += 1
        listing += 1
        
    return titlelist[st_list_count-1] 
#date 정보 파싱
def datemethod(st_list_count,html):
    article_datelist=[]
    datemarge=[]
    s1 = html.find_all("div", class_="cnn-search__results")
    article_date = s1[0].find_all("div", class_="cnn-search__result-publish-date")        
    for a in article_date:
        a=a.text.strip()
        article_datelist.append(a)
    
    #     trans daily month
    for tr_date in article_datelist:
        year=tr_date[7:12]
        year=year.strip()
        daily=tr_date[4:6]
        daily=daily.replace(',', '')
        if len(daily) == 1:
            daily='0'+daily
        else:
            daily
        if tr_date[0:3] == 'Jan':
            datemarge.append(year+'01'+daily)
        elif tr_date[0:3] == 'Feb':
            datemarge.append(year+'01'+daily)
        elif tr_date[0:3] == 'Mar':
            datemarge.append(year+'01'+daily)
        elif tr_date[0:3] == 'Apr':
            datemarge.append(year+'01'+daily)
        elif tr_date[0:3] == 'May':
            datemarge.append(year+'01'+daily)
        elif tr_date[0:3] == 'Jun':
            datemarge.append(year+'01'+daily)
        elif tr_date[0:3] == 'Jul':
            datemarge.append(year+'01'+daily)
        elif tr_date[0:3] == 'Aug':
            datemarge.append(year+'01'+daily)
        elif tr_date[0:3] == 'Sep':
            datemarge.append(year+'01'+daily)
        elif tr_date[0:3] == 'Oct':
            datemarge.append(year+'01'+daily)
        elif tr_date[0:3] == 'Nov':
            datemarge.append(year+'01'+daily)
        elif tr_date[0:3] == 'Dec':
            datemarge.append(year+'01'+daily)
    
    return datemarge[st_list_count-1]


#href 태그정보 파싱
def hrefmethod(st_list_count,html):
    titlelist=[]
    linklist=[]
    articlelist=[]
    s1 = html.find_all("div", class_="cnn-search__results")
    s2 = s1[0].find_all("a")

    idx = 0

    for i in s2:
        if idx % 2 == 1:
            articlelist.append(i)
        idx += 1

    order=0
    idx = 0
    listing = order * 10

    for i in articlelist:
        titlelist.append(i.text.strip())
        linklist.append("https:"+str(i).split("href=\"")[1].split("\">")[0])
        idx += 1
        listing += 1
    return linklist[st_list_count-1]

# 메인 함수 호출 pknumber= 데이터 수집 고유번호,urlquery= 데이터 수집 키워드 값,browser_path= 팬텀 js 설치 경로,order_input= 수집 시작값,order=수집 종료 값,delay = 페이지 로딩 시간)
def cnn_urlcrawler(pknumber,urlquery,browser_path,order_input,order,delay):
    try:
        news_flag="CNN"
        browser=webdriver.PhantomJS(browser_path)
        
        minPage=order_input
        maxPage=order
#         minPage=1
#         maxPage=7
        
        urllist=[]
        title_list=[]
        media_list=[]
        media_data_list=[]
        context_list=[]
        href_list=[]
        
        inputkey=urlquery

        for minPage in range(minPage,maxPage): 
            url="https://edition.cnn.com/search/?q=+"+str(inputkey)+"&size=10&from="+str(minPage * 10)+"&page="+str(minPage)
            urlt=urlOverlap(url)
            browser=webdriver.PhantomJS(browser_path)
            browser.get(urlt)
            browser.implicitly_wait(delay)

            source = browser.page_source
            html = BeautifulSoup(source, "html.parser")

            st_list_count=1
            dt_list_count=11

            print(url)
            for st_list_count in range(st_list_count,dt_list_count):
                title=titlemethod(st_list_count,html)
                title_list.append(title)
                dv.DatacolctVo.set_title_list(dv,title)

                href=hrefmethod(st_list_count,html)
                href_list.append(href)
                dv.DatacolctVo.set_href_list(dv,href)

                media_data=datemethod(st_list_count,html)
                media_data_list.append(media_data)

                context=content(st_list_count,html)
                context_list.append(context)
              
        return title_list,context_list,href_list,media_data_list,st_list_count,url,minPage,pknumber
        browser.close()
    except Exception  as e :
        print("Last page")
        print(e)
        return title_list,context_list,href_list,media_data_list,st_list_count,url,minPage,pknumber
        browser.close()
