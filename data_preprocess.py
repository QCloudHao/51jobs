#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @Time: 2020/4/6 19:23
# @Author: qyh

import numpy as np
import pandas as pd
import re
import jieba
import warnings

warnings.filterwarnings("ignore")

# 将岗位名称标准化：目前岗位名称太多太杂，需要统一
job_list = ['数据分析', "数据统计", "数据专员", '数据挖掘', '算法', '大数据',
            '开发工程师', '运营', '软件工程', '前端开发',
            '深度学习', 'ai', '数据库', '数据库', '数据产品',
            '客服', 'java', '.net', 'android', '人工智能', 'c++',
            '数据管理', "测试", "运维"]
job_list = np.array(job_list)
address_list = ['北京', '上海', '广州', '深圳', '杭州', '苏州', '长沙',
                '武汉', '天津', '成都', '西安', '东莞', '合肥', '佛山',
                '宁波', '南京', '重庆', '长春', '郑州', '常州', '福州',
                '沈阳', '济南', '宁波', '厦门', '贵州', '珠海', '青岛',
                '中山', '大连', '昆山', "惠州", "哈尔滨", "昆明", "南昌", "无锡"]
address_list = np.array(address_list)


def deduplicate(df):
    # 为数据框指定行索引
    df.index = range(len(df))
    df.columns = ["岗位名", "公司名", "工作地点", "工资", "发布日期", "经验与学历", "公司类型", "公司规模",
                  "行业", "工作描述"]
    # 数据去重，如果一个行业的公司名和发布的岗位名一致，我们认为是重复值，因此去除掉。
    print("去重之前的记录数", df.shape)
    df.drop_duplicates(subset=["公司名", "岗位名"], inplace=True)
    print("去重之后的记录数", df.shape)


def get_job_info(df):
    df["岗位名"].value_counts()
    df["岗位名"] = df["岗位名"].apply(lambda x: x.lower())
    # 数据太多太杂，我们需要筛选出我们想要分析的岗位（目标岗位）
    target_job = ['算法', '开发', '分析', '工程师', '数据', '运营', '运维']
    index = [df["岗位名"].str.count(i) for i in target_job]
    index = np.array(index).sum(axis=0) > 0
    job_info = df[index]
    job_info.shape

    return job_info


def handle_job_names(job_info):
    def rename(x=None, name_list=job_list):
        index = [i in x for i in name_list]
        if sum(index) > 0:
            return name_list[index][0]
        else:
            return x

    job_info["岗位名"] = job_info["岗位名"].apply(rename)
    job_info["岗位名"].value_counts()
    # 数据统计、数据专员、数据分析统一归为数据分析
    job_info["岗位名"] = job_info["岗位名"].apply(lambda x: re.sub("数据专员", "数据分析", x))
    job_info["岗位名"] = job_info["岗位名"].apply(lambda x: re.sub("数据统计", "数据分析", x))


def handle_salary(job_info):
    job_info["工资"].str[-1].value_counts()
    job_info["工资"].str[-3].value_counts()

    index1 = job_info["工资"].str[-1].isin(["年", "月"])
    index2 = job_info["工资"].str[-3].isin(["万", "千"])
    job_info = job_info[index1 & index2]

    def get_money_max_min(x):
        try:
            if x[-3] == "万":
                z = [float(i) * 10000 for i in re.findall("[0-9]+\.?[0-9]*", x)]
            elif x[-3] == "千":
                z = [float(i) * 1000 for i in re.findall("[0-9]+\.?[0-9]*", x)]
            if x[-1] == "年":
                z = [i / 12 for i in z]
            return z
        except:
            return x
    salary = job_info["工资"].apply(get_money_max_min)
    job_info["最低工资"] = salary.str[0]
    job_info["最高工资"] = salary.str[1]
    job_info["工资水平"] = job_info[["最低工资", "最高工资"]].mean(axis=1)


def handle_work_place(job_info):
    def rename(x=None, name_list=address_list):
        index = [i in x for i in name_list]
        if sum(index) > 0:
            return name_list[index][0]
        else:
            return x

    job_info["工作地点"] = job_info["工作地点"].apply(rename)


def handle_company_type(job_info):
    job_info.loc[job_info["公司类型"].apply(lambda x: len(x) < 6), "公司类型"] = np.nan
    job_info["公司类型"] = job_info["公司类型"].str[2:-2]


def handle_industry_tag(job_info):
    job_info["行业"] = job_info["行业"].apply(lambda x: re.sub(",", "/", x))
    job_info.loc[job_info["行业"].apply(lambda x: len(x) < 6), "行业"] = np.nan
    job_info["行业"] = job_info["行业"].str[2:-2].str.split("/").str[0]


def handle_exp_education(job_info):
    job_info["学历"] = job_info["经验与学历"].apply(lambda x: re.findall("本科|大专|应届生|在校生|硕士", x))

    def func(x):
        if len(x) == 0:
            return np.nan
        elif len(x) == 1 or len(x) == 2:
            return x[0]
        else:
            return x[2]

    job_info["学历"] = job_info["学历"].apply(func)


def handle_word_desc(job_info):
    with open(r".\stopword.txt", "r") as f:
        stopword = f.read()
    stopword = stopword.split()
    stopword = stopword + ["任职", "职位", " "]

    job_info["工作描述"] = job_info["工作描述"].str[2:-2].apply(lambda x: x.lower()).apply(lambda x: "".join(x)) \
        .apply(jieba.lcut).apply(lambda x: [i for i in x if i not in stopword])
    job_info.loc[job_info["工作描述"].apply(lambda x: len(x) < 6), "工作描述"] = np.nan


def handle_company_scale(job_info):
    def func(x):
        if x == "['少于50人']":
            return "<50"
        elif x == "['50-150人']":
            return "50-150"
        elif x == "['150-500人']":
            return '150-500'
        elif x == "['500-1000人']":
            return '500-1000'
        elif x == "['1000-5000人']":
            return '1000-5000'
        elif x == "['5000-10000人']":
            return '5000-10000'
        elif x == "['10000人以上']":
            return ">10000"
        else:
            return np.nan

    job_info["公司规模"] = job_info["公司规模"].apply(func)


if __name__ == '__main__':
    data = pd.read_csv("./job_info.csv", engine="python", header=None)
    deduplicate(data)
    job_info = get_job_info(data)

    handle_job_names(job_info)
    handle_salary(job_info)
    handle_work_place(job_info)
    handle_company_type(job_info)
    handle_industry_tag(job_info)
    handle_exp_education(job_info)
    handle_word_desc(job_info)
    handle_company_scale(job_info)
    feature = ["公司名", "岗位名", "工作地点", "工资", "发布日期", "学历", "公司类型", "公司规模", "行业", "工作描述"]
    final_df = job_info[feature]
    final_df.to_excel(r".\词云图.xlsx", encoding="gbk", index=None)

