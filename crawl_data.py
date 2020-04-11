#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Time: 2020/4/6 14:02
# @Author: qyh

import requests
import pandas as pd
from lxml import etree
import time
import warnings

warnings.filterwarnings("ignore")
url_pre = "https://search.51job.com/list/000000,000000,0000,00,9,99,%25E6%2595%25B0%25E6%258D%25AE,2,"
url_end = ".html?"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/73.0.3683.86 Safari/537.36'
}


def crawl_data_to_csv(page_num):
    for i in range(1, page_num):
        print("正在爬取第" + str(i) + "页的数据")
        url = url_pre + str(i) + url_end
        web = requests.get(url, headers)
        web.encoding = "gbk"
        dom = etree.HTML(web.text)
        # 1、岗位名称
        job_name = dom.xpath('//div[@class="dw_table"]/div[@class="el"]//p/span/a[@target="_blank"]/@title')
        # 2、公司名称
        company_name = dom.xpath(
            '//div[@class="dw_table"]/div[@class="el"]/span[@class="t2"]/a[@target="_blank"]/@title')
        # 3、工作地点
        address = dom.xpath('//div[@class="dw_table"]/div[@class="el"]/span[@class="t3"]/text()')
        # 4、工资
        salary_mid = dom.xpath('//div[@class="dw_table"]/div[@class="el"]/span[@class="t4"]')
        salary = [i.text for i in salary_mid]
        # 5、发布日期
        release_time = dom.xpath('//div[@class="dw_table"]/div[@class="el"]/span[@class="t5"]/text()')
        # 6、获取二级网址url
        deep_url = dom.xpath('//div[@class="dw_table"]/div[@class="el"]//p/span/a[@target="_blank"]/@href')
        rand_all_list = []
        job_describe_list = []
        company_type_list = []
        company_size_list = []
        industry_list = []
        for j in range(len(deep_url)):
            web_deep = requests.get(deep_url[j], headers=headers)
            web_deep.encoding = "gbk"
            dom_deep = etree.HTML(web_deep.text)
            # 7、爬取经验、学历信息，先合在一个字段里面，以后再做数据清洗
            random_all = dom_deep.xpath('//div[@class="tHeader tHjob"]//div[@class="cn"]/p[@class="msg ltype"]/text()')
            # 8、岗位描述性息
            job_describe = dom_deep.xpath('//div[@class="tBorderTop_box"]//div[@class="bmsg job_msg inbox"]/p/text()')
            # 9、公司类型
            company_type = dom_deep.xpath('//div[@class="tCompany_sidebar"]//div[@class="com_tag"]/p[1]/@title')
            # 10、公司规模(人数)
            company_size = dom_deep.xpath('//div[@class="tCompany_sidebar"]//div[@class="com_tag"]/p[2]/@title')
            # 11、所属行业(公司)
            industry = dom_deep.xpath('//div[@class="tCompany_sidebar"]//div[@class="com_tag"]/p[3]/@title')
            # 将上述信息保存到各自的列表中
            rand_all_list.append(random_all)
            job_describe_list.append(job_describe)
            company_type_list.append(company_type)
            company_size_list.append(company_size)
            industry_list.append(industry)

            time.sleep(1)
        # 由于我们需要爬取很多页，为了防止最后一次性保存所有数据出现的错误，因此，我们每获取一夜的数据，就进行一次数据存取。
        df = pd.DataFrame()
        df["岗位名称"] = job_name
        df["公司名称"] = company_name
        df["工作地点"] = address
        df["工资"] = salary
        df["发布日期"] = release_time
        df["经验、学历"] = rand_all_list
        df["公司类型"] = company_type_list
        df["公司规模"] = company_size_list
        df["所属行业"] = industry_list
        df["岗位描述"] = job_describe_list
        try:
            df.to_csv("job_info.csv", mode="a+", header=None, index=None, encoding="gbk")
            print("保存第" + str(i) + "页数据完成")
        except Exception:
            print("当页数据写入失败")
        time.sleep(1)
    print("数据爬取完毕！")


if __name__ == '__main__':
    crawl_data_to_csv(100)
