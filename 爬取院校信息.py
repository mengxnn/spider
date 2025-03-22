import time
import pandas as pd
import requests
import json
import os
from fake_useragent import UserAgent
import random
from concurrent.futures import ThreadPoolExecutor  # 多线程爬取
import threading
import sys
import pymysql  # 用于存入MySQL数据库

# 全局变量
college_info = []  # 存储院校信息
all_schools = [44,385,284,542,504,66,293,57,419,530,286,291,159]  # 存储所有学校ID
#all_provinces = [11, 12, 13, 14, 15, 21, 22, 23, 31, 32, 33, 34, 35, 36, 37, 41, 42, 43, 44, 45, 46, 50, 51, 52, 53, 54,61, 62, 63, 64, 65]  # 所有省份ID
all_provinces = [43]  # 所有省份ID

visited_schools = set()  # 记录已经爬取的院校，避免冗余

MAX_RETRIES = 5  # 最大允许连续失败次数
retry_count = 0   # 当前失败次数
save_lock = threading.Lock()  # 定义锁

#获取所有学校ID
def get_all_schools():
    url = "https://api.eol.cn/gkcx/api/?uri=apidata/api/gk/school/lists&page=1&size=5000"
    headers = {  #添加header字段，模拟用户登陆，防止被封ip
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        "Referer": "https://www.gaokao.cn/",
        "Accept": "application/json, text/plain, */*"
    }
    response = requests.get(url, headers=headers)
    time.sleep(random.uniform(4, 8))  # 限制请求频率，防止封禁
    #print("API 响应内容（前500字符）:", response.text[:500])  # 查看是否访问频繁
    if response.status_code == 200:
        data = response.json().get('data', {}).get('item', [])
        return [school['school_id'] for school in data]  # 返回所有school_id
    return []


#爬取院校信息
def spider_college_info(school_id, province_id):
    url = f"https://api.eol.cn/web/api/?local_province_id={province_id}&school_id={school_id}&uri=apidata/api/gk/score/province&year=2024"
    headers = {  # 添加header字段，模拟用户登陆，防止被封ip
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        "Referer": "https://www.gaokao.cn/",
        "Accept": "application/json, text/plain, */*"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        items = response.json().get('data', {}).get('item', [])
        #print(items)
        for school in items:
            school_info = {
                'school_name': school.get('name'),
                'school_id': school.get('school_id'),
                'is985': '是' if school.get('f985', 0) == 1 else '否',
                'is211': '是' if school.get('f211', 0) == 1 else '否',
                'isdoubleFC': '是' if school.get('dual_class_name') == "双一流" else '否'
            }
            college_info.append(school_info)
            print(school_info)
    else:
        print(f"⚠️ 请求失败: {response.status_code}")


def main():
    print(f"正在爬取 {len(all_schools)} 所院校的数据...")

    max_workers = 3  # 线程池最大线程数
    threadPool = ThreadPoolExecutor(max_workers)

    #爬取院校信息
    futures = []
    for school_id in all_schools:
        for province_id in all_provinces:
            futures.append(threadPool.submit(spider_college_info, school_id, province_id))
            time.sleep(random.uniform(4, 8))  # 请求间隔

    for future in futures:
        future.result()  # 等待所有任务完成

    threadPool.shutdown()
    print("数据爬取完成！")

    save_info_to_excel()  # 保存专业分数线数据


def save_info_to_excel():
    with save_lock:
        df_college_info = pd.DataFrame(college_info)

        # 使用 pandas DataFrame 来去重
        df_college_info = pd.DataFrame(college_info).drop_duplicates(subset=['school_id'])
        '''
        # 格式优化：将 985、211、双一流 字段转化为更易读的 '是/否'
        df_college_info['985'] = df_college_info['f985'].apply(lambda x: '是' if x == 1 else '否')
        df_college_info['211'] = df_college_info['f211'].apply(lambda x: '是' if x == 1 else '否')
        df_college_info['双一流'] = df_college_info['dual_class_name'].apply(lambda x: '是' if x == 1 else '否')
        '''
        df_college_info.to_excel("院校信息.xlsx", index=False)
        print("✅ 已成功保存院校信息到 '院校信息.xlsx'")



if __name__ == '__main__':
    main()
