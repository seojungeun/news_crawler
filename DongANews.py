from selenium import webdriver
import time
from bs4 import BeautifulSoup
import re
import urllib.parse
import DatacolctVo as dv
import psycopg2

#CLST 테이블 URL 정보 호출
def tblDataCstQery():
    try:
            keyword_query=[]
            #   Default port =1200
            conn_string = "host='121.160.16.80' dbname ='seoDB' user='dev' password='ne12345' port='12000'"
            conn = psycopg2.connect(conn_string)
            cur = conn.cursor()

            taget_query ="""
                    SELECT data_colct_url 
                    FROM   data_scraping_analysis.tbl_data_clst 
                    Where  data_clst_orig_no = '5'      
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


def existingURL():
    try:
            keyword_query=[]
            #   Default port =1200
            conn_string = "host='121.160.17.80' dbname ='EcoBank' user='dev' password='nie12345' port='12000'"
            conn = psycopg2.connect(conn_string)
            cur = conn.cursor()

            taget_query ="""
                    SELECT    
                        master.news_colct_url AS "newsColctUrl"
                    FROM   
                        data_scraping_analysis.tbl_news_colct AS master 
                    LEFT JOIN
                        data_scraping_analysis.tbl_data_clst AS sub
                    ON
                        master.news_clst_no = sub.data_clst_no
                    LEFT JOIN
                        data_scraping_analysis.tbl_data_clkw AS sub_clkw
                    ON
                        sub.data_clst_kwrd_no = sub_clkw.data_clkw_no
                    LEFT JOIN
                        data_scraping_analysis.tbl_data_clor AS sub_clor
                    ON
                        sub.data_clst_orig_no = sub_clor.data_clor_no

                    where sub_clor.data_clor_ttle = '동아일보'    
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

#데이터 중복 체크 
def urlOverlap(url):
    urlData=existingURL()
    for urlList in urlData:
        urlList=str(urlList)
        urlList=re.sub(",","",urlList)
        urlList=urlList.replace(")","")
        urlList=urlList.replace("(","")
        urlList=urlList.replace("'","")
        if  url == urlList:
            url=None
    return url

def urlGenerator(order_input,order,urlquery): 
    urllist=[]
    kw=urllib.parse.quote(urlquery)
    for order_input in range(order_input,order,15):
        url="http://news.donga.com/search?p="+str(order_input)+"&query="+str(kw)+"&check_news=1&more=1&sorting=1&search_date=1&v1=&v2=&range=1"
#         url=urlOverlap(url)
        urllist.append(url)
    return urllist
# 키워드 정보 인코딩
def keyword(urlquery): 
    Keyword=urllib.parse.quote(urlquery)
    return Keyword
#상세 페이지 파싱
def detail_page(href,browser_path):
    tmp = href.split('/')
    domain = tmp[2]

    class_name='article_txt'
    res=urllib.request.urlopen(href).read()
    html = BeautifulSoup(res, "html.parser")

    context = html.find_all("div", class_=class_name)

    content_list=[]
    for co in context:
        co =co .text.strip()
        content_list.append(co)

    return content_list

#date 파싱
def datemethod(st_list_count,html):
    date_list=[]
    for dates in html.find_all("p", class_='tit'):
        dates=dates.text.strip()
        date_list.append(re.search('\d{4}-\d{2}-\d{2} \d{2}:\d{2}',dates).group())
    return date_list[st_list_count-1]

#href 파싱
def hrefmethod(st_list_count,html):
    href_list=[]
    for i in html.find_all("p", class_="txt"):
        href_list.append(i.find("a").get('href'))
    return href_list[st_list_count-1]
#제목 파싱
def titlemethod(st_list_count,html):
    title_list=[]
    search_news = html.find_all("p", class_="tit")
    for search_news_st in search_news:
        title=search_news_st.text.strip()
    #     title_list.append(search_news_st.text.strip())
        title=re.sub('[0-9]','',title)
        title=title.replace('\n\n-- :','')
        title_list.append(title)
    return title_list[st_list_count-1] 
#매체정보 파싱
def bread_crumbsmethod(st_list_count,html):
    bread_list=[]
    bread_crumbs = html.find_all("div", class_="loc")
    for bread in bread_crumbs:
        bread =bread .text.strip()
        bread_list.append(bread)
    return bread_list[st_list_count-1]
# 메인 함수 호출 pknumber= 데이터 수집 고유번호,urlquery= 데이터 수집 키워드 값,browser_path= 팬텀 js 설치 경로,order_input= 수집 시작값,order=수집 종료 값,delay = 페이지 로딩 시간)
def dongamagazine(pknumber,urlquery,browser_path,order_input,order,delay):
    try:
        news_flag='DongAMagazine'
        browser=webdriver.PhantomJS(browser_path)
#         order_input=1
#         order=2

        urllist=[]
        title_list=[]
        media_list=[]
        media_data_list=[]
        context_list=[]
        href_list=[]

        kw=urllib.parse.quote(urlquery)
        for order_input in range(order_input,order,15):
            url="http://news.donga.com/search?p="+str(order_input)+"&query="+str(kw)+"&check_news=1&more=1&sorting=1&search_date=1&v1=&v2=&range=1&search_date=5&v1=&v2="
            urllist.append(url)

        for urlout in urllist:
            print(url)
            browser.get(url)
            browser.implicitly_wait(delay)
            source = browser.page_source
            html = BeautifulSoup(source, "html.parser")

            st_list_count=1
            dt_list_count=16

            for st_list_count in range(st_list_count,dt_list_count):
                try:
                    title=titlemethod(st_list_count,html)
                    title_list.append(title)
                    dv.DatacolctVo.set_title_list(dv,title)

                    media=bread_crumbsmethod(st_list_count,html)
                    media_list.append(media)
                    dv.DatacolctVo.set_media_data_list(dv,media)

                    href=hrefmethod(st_list_count,html)
                    href=urlOverlap(href)
                    if href == None:
                        print("데이터 중복")
                        raise
                    href_list.append(href)
                   

                    media_data=datemethod(st_list_count,html)
                    media_data_list.append(media_data)

                    context=detail_page(href,browser_path)
                    context_list.append(context)
                  
                except:     
                    pass

            return title_list,context_list,href_list,media_data_list,st_list_count,urlout,order_input,pknumber
            browser.close()

    except Exception  as e :
        print("Last page")
        print(e)
        return title_list,context_list,href_list,media_data_list,st_list_count,urlout,order_input,pknumber
        browser.close()
