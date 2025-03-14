import time
import pandas as pd
import requests
import json
import os
from fake_useragent import UserAgent
import random
from concurrent.futures import ThreadPoolExecutor  # 多线程爬取
import pymysql  # 用于存入MySQL数据库

# 全局变量
specialty_data_by_province = {}  # 存储不同省份的专业分数线数据
college_data = []  # 存储院校分数线
specialty_data = []  # 存储专业分数线
all_schools = [123,44]  # 存储所有学校ID
all_provinces = [11, 12, 13, 14, 15, 21, 22, 23, 31, 32, 33, 34, 35, 36, 37, 41, 42, 43, 44, 45, 46, 50, 51, 52, 53, 54,61, 62, 63, 64, 65]  # 所有省份ID
all_years = [2024, 2023, 2022]

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
            data = {
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
            print(data)
            college_data.append(data)
    time.sleep(random.uniform(3.7, 8))


# **Step 3: 爬取专业分数线**
def spider_specialty(school_id, province_id, year):
    """ 爬取指定学校、省份、年份的专业分数线数据 """
    base_url = "https://api.eol.cn/web/api/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        "Referer": "https://www.gaokao.cn/",
        "Accept":"application / json, text / plain, * / *",
    }
    page = 1  # 从第一页开始爬取
    size = 10  # 每页数据条数

    # 确保不同省份的数据存入各自的列表
    if province_id not in specialty_data_by_province:
        specialty_data_by_province[province_id] = []

    while True:
        url = f"{base_url}?local_province_id={province_id}&school_id={school_id}&uri=apidata/api/gk/score/special&year={year}&page={page}&size={size}"
        response = requests.get(url, headers=headers)
        time.sleep(random.uniform(3.7, 8))  # 限制请求频率，防止封禁
        if response.status_code != 200:
            print(f"⚠️ 请求失败，状态码: {response.status_code}")
            break  # 退出循环

        data = response.json().get('data', {})
        items = data.get('item', [])
        num_found = data.get('numFound', 0)  # 获取总数据条数

        if not items:
            print(f"✅ 爬取完成，{school_id} 在 {province_id} {year} 年没有更多数据")
            break  # 没有更多数据，退出循环

        for item in items:
            data=({
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
            print(data)
            specialty_data_by_province[province_id].append(data)
        print(f"✅ 已爬取 {school_id} 在 {province_id} {year} 年的第 {page} 页，共 {len(items)} 条数据")

        # 计算总页数
        total_pages = (num_found // size) + (1 if num_found % size else 0)

        # 如果当前页数达到总页数，停止循环
        if page >= total_pages:
            break

        page += 1  # 进入下一页


# **Step 4: 多线程爬取所有学校和省份数据**
def main():
    print(f"正在爬取 {len(all_schools)} 所院校的数据...")

    max_workers = 10  # 线程池最大线程数
    threadPool = ThreadPoolExecutor(max_workers)

    futures = []
    for year in all_years:
        for school_id in all_schools:
            for province_id in all_provinces:
                futures.append(threadPool.submit(spider_specialty, school_id, province_id, year))
                time.sleep(random.uniform(3.7, 8))  # 请求间隔

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
    """ 按省份存储数据，每个省单独保存到 `各省份专业分数线` 文件夹 """

    folder_path = "各省份专业分数线"
    os.makedirs(folder_path, exist_ok=True)  # 如果文件夹不存在，则创建

    for province_id, data in specialty_data_by_province.items():
        df = pd.DataFrame(data)
        if df.empty:
            continue  # 如果数据为空，则跳过

        province_name = df["招生省份"].iloc[0] if "招生省份" in df.columns and not df.empty else f"省份_{province_id}"
        filename = os.path.join(folder_path, f"{province_name}-专业分数线.xlsx")  # 存储到文件夹

        df.to_excel(filename, index=False)
        print(f"✅ {province_name} 的数据已保存到 {filename}")


if __name__ == '__main__':
    main()
