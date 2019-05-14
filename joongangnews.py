from selenium import webdriver
import time
from bs4 import BeautifulSoup
import re
import urllib.parse
import DatacolctVo as dv
import psycopg2

#CLST 테이블 URL URL 정보 호출
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
                    Where  data_clst_orig_no = '4'      
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

                    where sub_clor.data_clor_ttle = '중앙일보'    
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

# def keyword(urlquery): 
#     Keyword=urllib.parse.quote(urlquery)
#     return Keyword


#상세 페이지 파싱
def detail_page(href,browser_path):
    tmp = href.split('/')
    domain = tmp[2]
    class_name='article_body fs1 mg'
    res=urllib.request.urlopen(href).read()
    html = BeautifulSoup(res, "html.parser")
    context = html.find_all("div", class_=class_name)
    content_list=[]
    
    context[0:9000]
    for co in context:
        co =co .text.strip()
        content_list.append(co)
    return content_list
#date 파싱
def datemethod(st_list_count,html,browser):
    date_list=[]
    try:
        for dates in html.find_all("span", class_='byline'):
            dates=dates.text.strip()
            date_list.append(re.search('\d{4}.\d{2}.\d{2} \d{2}:\d{2}',dates).group())
            return date_list[st_list_count-1]
    except:
        date_list=[]
        browserdate=browser.find_element_by_xpath('//*[@id="content"]/div[2]/div[2]/ul/li['+str(st_list_count)+']/div/span[2]/em[2]').text
        return browserdate

#href 파싱
def hrefmethod(st_list_count,html):
    href_list=[]
    for i in html.find_all("strong", class_="headline mg"):
        try:
            href_list.append(i.find('a').get('href'))
        except:
            pass
    return href_list[st_list_count-1]

#제목 파싱
def titlemethod(st_list_count,html):
    title_list=[]
    search_news = html.find_all("strong", class_="headline mg")
    for search_news_st in search_news:
        for i in search_news_st.find_all("a"):
            title_list.append(i.text.strip())
    return title_list[st_list_count-1] 

#매체정보 파싱
def bread_crumbsmethod(st_list_count,html):
    bread_list=[]
    for bread in html.find_all("span", class_='byline'):
        bread =bread .text.strip()
        bread=bread.replace('|\n','')
        bread_list.append(re.sub('\d{4}.\d{2}.\d{2} \d{2}:\d{2}','',bread))

    return bread_list[st_list_count-1]

# 메인 함수 호출 pknumber= 데이터 수집 고유번호,urlquery= 데이터 수집 키워드 값,browser_path= 팬텀 js 설치 경로,order_input= 수집 시작값,order=수집 종료 값,delay = 페이지 로딩 시간)
def centermagazine(pknumber,urlquery,browser_path,order_input,order,delay):
    try:
        news_flag='Centermagazine'
        # browser_path="C:/Users/seo/Desktop/chromedrive/chromedriver.exe"
        browser=webdriver.PhantomJS(browser_path)
#         order_input=1
#         order=3

        Keyword=urllib.parse.quote(urlquery)
        urllist=[]
        title_list=[]
        media_list=[]
        media_data_list=[]
        context_list=[]
        href_list=[]


        for order_input in range(order_input,order):
#             url="https://search.joins.com/TotalNews?page="+str(order_input)+"&Keyword="+str(keyword)+"&SortType=New&SearchCategoryType=TotalNews"
            #   database Environment Setting
            url="https://search.joins.com/TotalNews?page="+str(order_input)+"&Keyword="+str(Keyword)+"&SortType=New&SearchCategoryType=TotalNews"
            browser.get(url)
            browser.implicitly_wait(delay)

            source = browser.page_source
            html = BeautifulSoup(source, "html.parser")

            st_list_count=1
            dt_list_count=10


            print(url)
            for st_list_count in range(st_list_count,dt_list_count):

                title=titlemethod(st_list_count,html)
                title_list.append(title)
                dv.DatacolctVo.set_title_list(dv,title)

                media=bread_crumbsmethod(st_list_count,html)
                media_list.append(media)
                dv.DatacolctVo.set_media_data_list(dv,media)

                href=hrefmethod(st_list_count,html)
                if href == None:
                    print("데이터 중복")
                    raise
                href_list.append(href)
                dv.DatacolctVo.set_href_list(dv,href)

                media_data=datemethod(st_list_count,html,browser)
                media_data_list.append(media_data)

                context=detail_page(href,browser_path)
                context_list.append(context)
                dv.DatacolctVo.set_context_list(dv,context)

            dv.DatacolctVo.set_st_list_count(dv,st_list_count)
            dv.DatacolctVo.set_url_data(dv,url)
            dv.DatacolctVo.set_order_input(dv,order_input)
            time.sleep(delay)
        return title_list,context_list,href_list,media_data_list,st_list_count,url,order_input,pknumber
    except Exception  as e :
        print("Last page")
        print(e)
        return title_list,context_list,href_list,media_data_list,st_list_count,url,order_input,pknumber
        browser.close()


