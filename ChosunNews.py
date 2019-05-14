from selenium import webdriver
import time
from bs4 import BeautifulSoup
import urllib.parse
import DatacolctVo as dv
import psycopg2
#CLST 테이블에서 수집 된 URL 정보 호출
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
                    Where  data_clst_orig_no = '3'      
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
#URL 중복 체크                     
def urlOverlap(url):
    urlData=tblDataCstQery()
    if not urlData== url:
        return url


#키워드 인코딩
def keyword(urlquery): 
    Keyword=urllib.parse.quote(urlquery)
    return Keyword
#목록 파싱
def content(href,class_name,browser_path):
    res=urllib.request.urlopen(href).read()
    html = BeautifulSoup(res, "html.parser")
    context = html.find_all("div", class_=class_name)

    content_list=[]
    for co in context:
        co =co .text.strip()
        content_list.append(co)
        
    return content_list
#상세 페이지 파싱
def detail_page(href,browser_path):
    tmp = href.split('/')
    domain = tmp[2]

    append_domainlist=[]
    append_href=[]

    domainlist=['biz.chosun.com'+'news.chosun.com'+ 'premium.chosun.com'+'travel.chosun.com'+'san.chosun.com' ]
    for doun in domainlist:
        if domain==doun:
            class_name='par'
            content(href,class_name,browser_path)
            return content(href,class_name,browser_path)
        elif domain=='kid.chosun.com' :
            class_name='Paragraph'
            content(href,class_name,browser_path)
            return content(href,class_name,browser_path)
            
        elif domain == 'boomup.chosun.com' :
            class_name='article'
            content(href,class_name,browser_path)
            return content(href,class_name,browser_path)
        
        elif domain =='baby.chosun.com':
            class_name='newsCnt'
            content(href,class_name,browser_path)
            return content(href,class_name,browser_path)

        else:
            class_name='par'
            content(href,class_name,browser_path)
            append_domainlist.append(domain)
            append_href.append(href)
            return content(href,class_name,browser_path)

# date 파싱
def datemethod(st_list_count,html):
    date_list=[]
    datatest = html.find_all("span", class_='date')
    for date_ in datatest:
        date_ =date_ .text.strip()
        date_list.append(date_)
    return date_list[st_list_count-1]
#href 파싱
def hrefmethod(st_list_count,html):
    href_list=[]
    for i in html.find_all("dd", class_="thumb"):
        href_list.append(i.find("a").get('href'))
    return href_list[st_list_count-1]
#제목 파싱
def titlemethod(st_list_count,html):
    title_list=[]
    search_news = html.find_all("dl", class_="search_news")
    for search_news_st in search_news:
        for i in search_news_st.find_all("dt"):
            title_list.append(i.text.strip())
    return title_list[st_list_count-1] 
#뉴스 매체정보 파싱
def bread_crumbsmethod(st_list_count,html):
    bread_list=[]
    bread_crumbs = html.find_all("span", class_="bread_crumbs")
    for bread in bread_crumbs:
        bread =bread .text.strip()
        bread_list.append(bread)
    return bread_list[st_list_count-1]
# 메인 함수 호출 pknumber= 데이터 수집 고유번호,urlquery= 데이터 수집 키워드 값,browser_path= 팬텀 js 설치 경로,order_input= 수집 시작값,order=수집 종료 값,delay = 페이지 로딩 시간)
def chosunmagazine(pknumber,urlquery,browser_path,order_input,order,delay):
    try:
        news_flag="chosunmagazine"
#         order_input=1
#         order=10
        urllist=[]
        title_list=[]
        media_list=[]
        media_data_list=[]
        context_list=[]
        href_list=[]
        
        kw=urllib.parse.quote(urlquery)
#         kw=keyword(urlquery)
        
        for order_input in range(order_input,order):
            url='http://search.chosun.com/search/news.search?query='+str(kw)+'&pageno='+str(order_input)+'&orderby=news&naviarraystr=&kind=&cont1=&cont2=&cont5=&categoryname=&categoryd2=&c_scope=more_news&sdate=&edate=&premium='
            urlt=urlOverlap(url)
            browser=webdriver.PhantomJS(browser_path)
#             for url_data in urllist:     
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
                
                media=bread_crumbsmethod(st_list_count,html)
                media_list.append(media)
                dv.DatacolctVo.set_media_data_list(dv,media)

                href=hrefmethod(st_list_count,html)
                href_list.append(href)
                dv.DatacolctVo.set_href_list(dv,href)

                media_data=datemethod(st_list_count,html)
                media_data_list.append(media_data)

                context=detail_page(href,browser_path)
                context_list.append(context)
                dv.DatacolctVo.set_context_list(dv,context)
                
            dv.DatacolctVo.set_st_list_count(dv,st_list_count)
            dv.DatacolctVo.set_url_data(dv,url)
            dv.DatacolctVo.set_order_input(dv,order_input)
        return title_list,context_list,href_list,media_data_list,st_list_count,url,order_input,pknumber
    except Exception  as e :
        print("Last page")
        print(e)
        return title_list,context_list,href_list,media_data_list,st_list_count,url,order_input,pknumber
        browser.close()
