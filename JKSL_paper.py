from selenium import webdriver
import time
import re
from urllib.request import urlretrieve
from bs4 import BeautifulSoup
import datetime
import psycopg2
import pathlib



# URL 중복 체크  
def urlOverlap(url):
    urlData=tblDataCstQery()
    if not urlData== url:
        return url
# 파일 다운로드 
def parser(bx,li_count,file_path,info_trim,urlquery):
    button=bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[4]/a[1]')
    button_attr=button.get_attribute('href')
    button_trim=button_attr.replace("javascript:downloadPDF","")
    button_trim=button_trim.replace(";","")
    button_trim=button_trim.replace("(","")
    button_trim=button_trim.replace(")","")
    button_trim=button_trim.replace("'","")

    download='http://fulltext.koreascholar.com/Service/Download.aspx?pdf='+button_trim
    
    pathlib.Path(file_path+urlquery+"/").mkdir(parents=True, exist_ok=True)     
    file_dir=file_path+urlquery+"/"+info_trim+".pdf"
    
    print(file_dir)
    if file_dir is not None:
        dwld_co=1 
        urlretrieve(download, file_dir)
    else :
        dwld_co=0
    return dwld_co
#DB URL 정보 호출 
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
                
#메인 함수 부분(pkumber = 데이터 수집 고유번호,browser_path = 셀리니움 설치 경로,csv_output= 논문 다운로드 저장 경로,order_input=수집 페이지 첫 지점,order=수집 페이지 끝 지점,delay=페이지 로딩 시간)
def paper(pknumber,urlquery,browser_path,csv_output,order_input,order,delay):
    title_arr=[]
    author_arr=[]
    info_arr=[]
    link_arr=[]
    abstract_arr=[]
    download_arr=[]
    href_arr =[] 
    date_arr=[]
    dwld_co_arr=[]
    info_trim_arr=[]
    
    
    try:
        url_first_count=order_input
        url_last_count=order
        
        now = datetime.datetime.now()
        startDate = now.strftime('%Y%m%d')
                
        user_key=urlquery

        file_path=csv_output+startDate+'/'
        browser=webdriver.PhantomJS(browser_path)
        thss_flag='JKSL'
        
        bx=browser.find_element_by_xpath       
        for url_first_count in range (url_first_count,url_last_count):   
            url='http://www.jksl.or.kr/journal/list.php?m=1&search='+str(user_key)+'&Page='+str(url_first_count)+''
            urlOvl = urlOverlap(url)
        
            browser.get(urlOvl)
            browser.implicitly_wait(delay)
            html = browser.page_source
            li_count=1
            print(url)    
            for li_count in range (li_count,12):
                info=bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[3]').text
                info_trim = info.replace("Korean Journal of Ecology and Environment :: ","")
                info_trim = info_trim.strip()

                info_trim_arr.append(info_trim)

                tmp = info_trim.split(' ')
                date_check=tmp[0]

                if date_check== 'Vol.51':
                    pblcdate='2018'
                    pr=parser(bx,li_count,file_path,info_trim,urlquery)
                    dwld_co_arr.append(pr)

                    info=bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[3]').text
                    title=bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[1]/a').text
                    author=bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[2]').text


                    #  abstract open             
                    bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/a').click()
                    abstract= bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/div').text

                    #  DOI 
                    data_check=browser.find_element_by_xpath('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[1]/a')
                    href=data_check.get_attribute("href")   

                    href_arr.append(href)
                    title_arr.append(title) 
                    author_arr.append(author) 
                    info_arr.append(info) 
                    abstract_arr.append(abstract)
                    date_arr.append(pblcdate)

                elif date_check=='Vol.50': 
                    pblcdate='2017'
                    pr=parser(bx,li_count,file_path,info_trim,urlquery)
                    dwld_co_arr.append(pr)

                    info=bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[3]').text
                    title=bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[1]/a').text
                    author=bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[2]').text


                    #  abstract open             
                    bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/a').click()
                    abstract= bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/div').text

                    #  DOI 
                    data_check=browser.find_element_by_xpath('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[1]/a')
                    href=data_check.get_attribute("href")   

                    href_arr.append(href)
                    title_arr.append(title) 
                    author_arr.append(author) 
                    info_arr.append(info) 
                    abstract_arr.append(abstract)
                    date_arr.append(pblcdate)

                elif date_check=='Vol.49':
                    pblcdate='2016'
                    pr=parser(bx,li_count,file_path,info_trim,urlquery)
                    dwld_co_arr.append(pr)

                    info=bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[3]').text
                    title=bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[1]/a').text
                    author=bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[2]').text


                    #  abstract open             
                    bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/a').click()
                    abstract= bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/div').text

                    #  DOI 
                    data_check=browser.find_element_by_xpath('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[1]/a')
                    href=data_check.get_attribute("href")   

                    href_arr.append(href)
                    title_arr.append(title) 
                    author_arr.append(author) 
                    info_arr.append(info) 
                    abstract_arr.append(abstract)
                    date_arr.append(pblcdate)

                elif date_check=='Vol.48':
                    pblcdate='2015'
                    pr=parser(bx,li_count,file_path,info_trim,urlquery)
                    dwld_co_arr.append(pr)

                    info=bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[3]').text
                    title=bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[1]/a').text
                    author=bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[2]').text


                    #  abstract open             
                    bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/a').click()
                    abstract= bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/div').text

                    #  DOI 
                    data_check=browser.find_element_by_xpath('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[1]/a')
                    href=data_check.get_attribute("href")   

                    href_arr.append(href)
                    title_arr.append(title) 
                    author_arr.append(author) 
                    info_arr.append(info) 
                    abstract_arr.append(abstract)
                    date_arr.append(pblcdate)

                elif date_check=='Vol.47':
                    pblcdate='2014'
                    pr=parser(bx,li_count,file_path,info_trim,urlquery)
                    dwld_co_arr.append(pr)

                    info=bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[3]').text
                    title=bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[1]/a').text
                    author=bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[2]').text


                    #  abstract open             
                    bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/a').click()
                    abstract= bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/div').text

                    #  DOI 
                    data_check=browser.find_element_by_xpath('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[1]/a')
                    href=data_check.get_attribute("href")   

                    href_arr.append(href)
                    title_arr.append(title) 
                    author_arr.append(author) 
                    info_arr.append(info) 
                    abstract_arr.append(abstract)
                    date_arr.append(pblcdate)

                elif date_check=='Vol.46':
                    pblcdate='2013'
                    pr=parser(bx,li_count,file_path,info_trim,urlquery)
                    dwld_co_arr.append(pr)

                    info=bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[3]').text
                    title=bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[1]/a').text
                    author=bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[2]').text


                    #  abstract open             
                    bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/a').click()
                    abstract= bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/div').text

                    #  DOI 
                    data_check=browser.find_element_by_xpath('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[1]/a')
                    href=data_check.get_attribute("href")   

                    href_arr.append(href)
                    title_arr.append(title) 
                    author_arr.append(author) 
                    info_arr.append(info) 
                    abstract_arr.append(abstract)
                    date_arr.append(pblcdate)

                elif date_check=='Vol.45':
                    pblcdate='2012'
                    pr=parser(bx,li_count,file_path,info_trim,urlquery)
                    dwld_co_arr.append(pr)

                    info=bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[3]').text
                    title=bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[1]/a').text
                    author=bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[2]').text


                    #  abstract open             
                    bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/a').click()
                    abstract= bx('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/div').text

                    #  DOI 
                    data_check=browser.find_element_by_xpath('//*[@id="content"]/div[2]/ul/li['+str(li_count)+']/p[1]/a')
                    href=data_check.get_attribute("href")   

                    href_arr.append(href)
                    title_arr.append(title) 
                    author_arr.append(author) 
                    info_arr.append(info) 
                    abstract_arr.append(abstract)
                    date_arr.append(pblcdate)

                else :
                    pblcdate=''
                    print('NoneType')
        return title_arr,abstract_arr,href_arr, date_arr, author_arr, dwld_co_arr,url_first_count,url,li_count,pknumber
        browser.close()
    except Exception as e:
        pass
        
    
    

