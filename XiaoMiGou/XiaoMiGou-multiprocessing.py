'''
今天作业：小咪购网站优惠券信息采集
目标网址：http://m.hlxns.com/m/index.php?r=l&u=427272
采集字段内容：商品类型,商品名称,优惠券,券后价,在售价,销量,推荐理由,优惠券链接
要求：采集并对数据进行清洗，清洗完成存储至本地 mongodb
'''

import random
import time, datetime
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
# 头信息
headers = {
    'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36"}
# 无头浏览器
option = ChromeOptions()
# option.add_argument('--headless')  # 无界面浏览器的设置
driver = webdriver.Chrome()
driver.implicitly_wait(30)  # 隐性等待，最长等待30秒


def pages_all(url):
    '''
    获取全部商品页码总和
    :param url:
    :return:
    '''
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            response.encoding = "utf-8"  # 设置编码
            page = etree.HTML(response.text).xpath('//a[@class="item"]/text()')[-1]
            return page
    except requests.RequestException:
        print('无法获取总页码')


def next_page(pages):
    '''
    设置爬取页数，获取商品列
    :param pages:
    :return:
    '''
    try:
        for page in range(1, int(pages) + 1):
            print('正在爬取第%s页' % (page))
            cur_url = BASE_URL + "&page=" + str(page)
            driver.get(cur_url)
            if cur_url:
                url_sps = driver.find_elements_by_xpath('//li[contains(@class," g_over")]/a')
                yield [url_sp.get_attribute("href") for url_sp in url_sps]
    finally:
        driver.close()
        print('浏览器关闭')


def detail_info(url):
    '''
    提取详情信息
    :param url_s:
    :return:
    '''
    time.sleep(random.random() * 2)
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        response.encoding = 'urf-8'
        html = etree.HTML(response.text)
        # 标题
        title = html.xpath('//div[@class="detail-col"]/a/span[@class="title"]/text()')[0]
        # 商品类别
        catgory = html.xpath('//div[contains(@class,"nav-wrap")]/div/a[3]/text()')[0]
        # 推荐理由
        summary1 = html.xpath('//div[@class="goods-desc cf"]/div[@class="content"]/p//text()')
        if summary1:
            summary2 = ''.join(summary1)
        summary3 = html.xpath('//div[@class="rec-text"]/span/text()')
        summary = summary2 + summary3[0]
        # 券后价
        now_price = html.xpath('//div[contains(@class,"price")]/i/text()')[0]
        # 原价
        old_price = html.xpath('//div[contains(@class,"price")]/b/i/text()')[0]
        # 销量
        num = html.xpath('//span[@class="num"]/i/text()')[0]
        nums_unit = html.xpath('//span[@class="num"]//text()')[2].strip()
        nums = int(float(num) * 10000) if nums_unit == '万件' else num
        # 优惠券
        coupon_price = html.xpath('//div[contains(@class,"coupon")]/span/text()')[0]
        # 优惠券地址
        coupon_url = html.xpath('//div[contains(@class,"buy-share")]/a/@href')[0]
        # 更新时间
        update_time = datetime.datetime.now().strftime('%Y') + '-' + \
                      html.xpath('//div[@class="time-num"]/span[@class="time"]/text()')[0].split('：')[1]
        data = {
            'title': title,
            'summary': summary,
            'catgory': catgory,
            'now_price': now_price,
            'old_price': old_price,
            'nums': nums,
            'coupon_price': coupon_price,
            'coupon_url': coupon_url,
            'update_time': update_time
        }
        save_data(data)


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
    pool = multiprocessing.Pool(3)
    pages = pages_all(BASE_URL)
    for ls in next_page(pages):
        pool.map(detail_info, ls)
    pool.close()
