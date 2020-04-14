'''
今天作业：小咪购网站优惠券信息采集
目标网址：http://m.hlxns.com/m/index.php?r=l&u=427272
采集字段内容：商品类型,商品名称,优惠券,券后价,在售价,销量,推荐理由,优惠券链接
要求：采集并对数据进行清洗，清洗完成存储至本地 mongodb
'''

import random
import time
import requests
import pymongo
import multiprocessing
from lxml import etree
from selenium import webdriver
from selenium.webdriver import ChromeOptions

BASE_URL = 'http://m.hlxns.com/m/index.php?r=l&u=427272'
# 设置MongoDB相应的参数
MONGO_CONNECTTON_STRING = 'mongodb://localhost:27017'
MONGO_DB_NAME = 'xiaomigou'
MONGO_CONNECTTON_NAME = 'xiaomigou'
# 连接MongoDB，并常见数据库和集合
client = pymongo.MongoClient(MONGO_CONNECTTON_STRING)
db = client[MONGO_DB_NAME]
collention = db[MONGO_CONNECTTON_NAME]


def pages_all(url):
    '''
    获取全部商品页码总和
    :param url:
    :return:
    '''
    headers = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36"}
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            response.encoding = "utf-8"  # 设置编码
            page = etree.HTML(response.text).xpath('//a[@class="item"]/text()')[-1]
            return page
    except requests.RequestException:
        print('无法获取总页码')


def chrome_option(url):
    '''
    设置Chrome浏览器
    :param url:
    :return:
    '''
    option = ChromeOptions()
    option.add_argument('--headless')  # 无界面浏览器的设置
    driver = webdriver.Chrome(options=option)
    driver.implicitly_wait(30)  # 隐性等待，最长等待30秒
    driver.get(url)
    return driver


def next_page(pages):
    '''
    设置爬取页数，获取商品列
    :param pages:
    :return:
    '''
    for page in range(1, int(pages) + 1):
        print('正在爬取第%s页' % (page))
        cur_url = BASE_URL + "&page=" + str(page)
        driver = chrome_option(cur_url)
        if cur_url == driver.current_url:
            url_sps = driver.find_elements_by_xpath('//li[contains(@class," g_over")]/a')
            detail_list(url_sps)
        driver.quit()
        print('浏览器关闭')


def detail_list(url_sps):
    '''
    获取每页全部详情链接
    :param url_sps:
    :return:
    '''
    ls = []
    for url_sp in url_sps:
        new_url = url_sp.get_attribute("href")
        ls.append(new_url)
    # print(ls)
    mult_pool(ls)


def detail_info(url_s):
    '''
    提取详情信息
    :param url_s:
    :return:
    '''
    time.sleep(random.random() * 1)
    driver2 = chrome_option(url_s)
    # 标题
    info = driver2.find_elements_by_xpath('//span[contains(@class,"title")]')  # 产品
    title = info[0].text
    # 推荐理由
    summary = driver2.find_elements_by_xpath('//div[contains(@class,"goods-desc")]')  # 推荐理由
    summary = summary[0].find_element_by_class_name("content").text + summary[0].find_element_by_class_name(
        "rec-text").text
    # 商品类别
    catgory = driver2.find_elements_by_xpath('//div[contains(@class,"nav-wrap")]/div/a[3]')[0].text
    # 券后价
    now_price = driver2.find_elements_by_xpath('//div[contains(@class,"price")]/i')[0].text
    # 原价
    old_price = driver2.find_elements_by_xpath('//div[contains(@class,"price")]/b/i')[0].text
    # 销量
    nums = driver2.find_element_by_css_selector(".num i").text
    # 优惠券
    coupon = driver2.find_elements_by_xpath('//div[contains(@class,"coupon")]/span')[0].text
    # 优惠券地址
    sp_url = driver2.find_elements_by_xpath('//div[contains(@class,"buy-share")]/a')
    sp_url = sp_url[0].get_attribute("href")
    driver2.quit()
    data = {
        'title': title,
        'summary': summary,
        'catgory': catgory,
        'now_price': now_price,
        'old_price': old_price,
        'nums': nums,
        'coupon': coupon,
        'sp_url': sp_url
    }
    save_data(data)


def mult_pool(ls):
    '''
    设置多线程爬取
    :param ls:
    :return:
    '''
    pool = multiprocessing.Pool(3)
    pool.map(detail_info, ls)
    pool.close()


def save_data(data):
    '''
    保存到mongodb
    :param data:
    :return:
    '''
    collention.update_one({
        'title': data.get('title')
    }, {
        '$set': data  # $set操作符表示更新操作
    }, upsert=True)  # upsert设为True，表示存在即更新，不存在即插入
    print('写入完成！')


if __name__ == '__main__':
    pages = pages_all(BASE_URL)
    next_page(pages)
