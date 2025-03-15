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
specialty_data_by_province = {}  # 存储不同省份的专业分数线数据
college_data = []  # 存储院校分数线
specialty_data = []  # 存储专业分数线
all_schools = [44,385,284,542,504,66,293,57,419,530,286,291,159]  # 存储所有学校ID
#all_provinces = [11, 12, 13, 14, 15, 21, 22, 23, 31, 32, 33, 34, 35, 36, 37, 41, 42, 43, 44, 45, 46, 50, 51, 52, 53, 54,61, 62, 63, 64, 65]  # 所有省份ID
all_provinces = [15,43,44,50]  # 所有省份ID
all_years = [2023,2022,2021]

MAX_RETRIES = 5  # 最大允许连续失败次数
retry_count = 0   # 当前失败次数
save_lock = threading.Lock()  # 定义锁

# **Step 1: 获取所有学校ID**
def get_all_schools():
    url = "https://api.eol.cn/gkcx/api/?uri=apidata/api/gk/school/lists&page=1&size=5000"
    headers = {  #添加header字段，模拟用户登陆，防止被封ip
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Referer": "https://www.gaokao.cn/",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
    }
    response = requests.get(url, headers=headers)
    time.sleep(random.uniform(3, 8))  # 限制请求频率，防止封禁
    print("API 响应内容（前500字符）:", response.text[:500])  # 查看是否访问频繁
    if response.status_code == 200:
        data = response.json().get('data', {}).get('item', [])
        return [school['school_id'] for school in data]  # 返回所有school_id
    return []


# **Step 2: 爬取院校分数线**
def spider_college(school_id, province_id):
    url = f"https://api.eol.cn/web/api/?local_province_id={province_id}&school_id={school_id}&uri=apidata/api/gk/score/province&year=2024"
    headers = {
        "User-Agent": UserAgent().random
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        items = response.json().get('data', {}).get('item', [])
        for item in items:
            data1 = {
                '年份': item.get('year'),
                '院校名称': item.get('name'),
                '院校id': item.get('school_id'),
                '招生省份': item.get('local_province_name'),
                '录取批次': item.get('local_batch_name'),
                '专业组': item.get('sg_name'),
                '选科大类':item.get('local_type_name'),
                '选科要求': item.get('sg_info'),
                '最低分/最低位次': str(item.get('min')) + '/' + str(item.get('min_section')),
                '省控线': item.get('proscore'),
            }
            print(data1)
            college_data.append(data1)
    time.sleep(random.uniform(4 ,8))


# **Step 3: 爬取专业分数线**
def spider_specialty(school_id, province_id, year):
    """ 爬取指定学校、省份、年份的专业分数线数据 """
    global retry_count  # 允许函数修改全局变量
    base_url = "https://api.eol.cn/web/api/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        "Referer": "https://www.gaokao.cn/",
        "Accept":"application / json, text / plain, * / *",
    }
    page = 1  # 从第一页开始爬取
    size = 10  # 每页数据条数

    # 确保不同省份的数据存入各自的列表
    if (province_id, year) not in specialty_data_by_province:
        specialty_data_by_province[(province_id, year)] = []

    while True:
        url = f"{base_url}?local_province_id={province_id}&school_id={school_id}&uri=apidata/api/gk/score/special&year={year}&page={page}&size={size}"
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f"⚠️ 请求失败: {response.status_code}")
            retry_count += 1
        else:
            retry_count = 0  # 成功请求后重置失败计数

        # 处理访问过于频繁的情况
        try:
            data = response.json()
            #print(data)  {'code': '1069', 'message': '访问太过频繁，请稍后再试', 'data': '222.240.53.107', 'location': '', 'encrydata': ''}
            if data.get("code") == "1069":
                print("⚠️ 访问过于频繁，正在保存数据并退出...")
                save_to_excel()
                sys.exit(0)  # 终止程序

        except Exception as e:
            print(f"❌ 解析 JSON 失败: {str(e)}")
            retry_count += 1

        # 失败次数超过阈值，自动保存并退出
        if retry_count >= MAX_RETRIES:
            print("⚠️ 失败次数过多，保存数据并退出...")
            save_to_excel()
            sys.exit(0)

        data = response.json().get('data', {})
        items = data.get('item', [])
        num_found = data.get('numFound', 0)  # 获取总数据条数

        if not items:
            print(f"✅ 爬取完成，{school_id} 在 {province_id} {year} 年没有更多数据")
            break  # 没有更多数据，退出循环

        for item in items:
            data1=({
                '年份': year,
                '院校id': item.get('school_id'),
                '院校名称': item.get('name'),
                '招生省份': item.get('local_province_name'),
                '录取批次': item.get('local_batch_name'),
                '选科大类': item.get('local_type_name'),
                '选科要求': item.get('sg_info'),
                '专业名称': item.get('level3_name'),
                '最高分': item.get('max'),
                '最低分/最低位次': str(item.get('min')) + '/' + str(item.get('min_section')),
            })
            print(data1)
            specialty_data_by_province[(province_id, year)].append(data1)
        print(f"✅ 已爬取 {school_id} 在 {province_id} {year} 年的第 {page} 页，共 {len(items)} 条数据")

        # 计算总页数
        total_pages = (num_found // size) + (1 if num_found % size else 0)
        # 如果当前页数达到总页数，停止循环
        if page >= total_pages:
            break

        page += 1  # 进入下一页
        time.sleep(random.uniform(4, 8))  # 限制请求频率，防止封禁


# **Step 4: 多线程爬取所有学校和省份数据**
def main():
    print(f"正在爬取 {len(all_schools)} 所院校的数据...")

    max_workers = 4  # 线程池最大线程数
    threadPool = ThreadPoolExecutor(max_workers)

    futures = []
    for year in all_years:
        for school_id in all_schools:
            for province_id in all_provinces:
                futures.append(threadPool.submit(spider_specialty, school_id, province_id, year))
                time.sleep(random.uniform(4, 8))  # 请求间隔

    for future in futures:
        future.result()  # 等待所有任务完成

    threadPool.shutdown()

    save_to_excel()  # 保存数据
    print("数据爬取完成！")


# **Step 5: 存入 MySQL 数据库**
'''
def save_to_db():
    connection = pymysql.connect(host='localhost', user='root', password='root', database='gaokao', charset='utf8mb4')
    cursor = connection.cursor()

    # 创建表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS college_scores (
            id INT AUTO_INCREMENT PRIMARY KEY,
            school_id INT,
            province_id INT,
            year INT,
            batch VARCHAR(50),
            type VARCHAR(50),
            min_score INT,
            min_rank INT,
            admission_rate VARCHAR(20)
        );
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS specialty_scores (
            id INT AUTO_INCREMENT PRIMARY KEY,
            school_id INT,
            province_id INT,
            major VARCHAR(100),
            batch VARCHAR(50),
            min_score INT,
            min_rank INT,
            admission_rate VARCHAR(20)
        );
    """)

    # 插入数据
    for data in college_data:
        cursor.execute(
            "INSERT INTO college_scores (school_id, province_id, year, batch, type, min_score, min_rank, admission_rate) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (data['school_id'], data['province_id'], data['year'], data['batch'], data['type'], data['min_score'],
             data['min_rank'], data['admission_rate']))

    for data in specialty_data:
        cursor.execute(
            "INSERT INTO specialty_scores (school_id, province_id, major, batch, min_score, min_rank, admission_rate) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (data['school_id'], data['province_id'], data['major'], data['batch'], data['min_score'], data['min_rank'],
             data['admission_rate']))

    connection.commit()
    cursor.close()
    connection.close()
    print("数据已存入数据库")
'''

# **Step 6: 存入 Excel**
def save_to_excel():
    """ 按年份、省份分类存储数据 """

    with save_lock:  # 确保只有一个线程执行此函数，保存数据
        base_folder = "各省份专业分数线"
        os.makedirs(base_folder, exist_ok=True)  # 确保目录存在

        for key, data in specialty_data_by_province.items():
            if isinstance(key, tuple) and len(key) == 2:
                province_id, year = key
            else:
                print(f"⚠️ 错误数据格式: {key}, 跳过存储")
                continue

            df = pd.DataFrame(data)
            if df.empty:
                continue

            province_name = df["招生省份"].iloc[
                0] if "招生省份" in df.columns and not df.empty else f"省份_{province_id}"
            province_folder = os.path.join(base_folder, f"{province_id}-{province_name}-专业分数线")
            os.makedirs(province_folder, exist_ok=True)

            filename = os.path.join(province_folder, f"{year}年专业分数线.xlsx")
            df.to_excel(filename, index=False)

            print(f"✅ {province_name} {year} 数据已保存到 {filename}")


if __name__ == '__main__':
    main()
