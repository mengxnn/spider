import time
import pandas as pd
import requests
import os
from fake_useragent import UserAgent
import random
from concurrent.futures import ThreadPoolExecutor  # 多线程爬取
import threading
import pymysql  # 用于存入MySQL数据库
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, String  # 新增导入类型

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',
    'database': 'score',
    'charset': 'utf8mb4'
}

# 全局变量
specialty_data_by_province = {}  # 存储不同省份的专业分数线数据
college_data = []  # 存储院校分数线
specialty_data = []  # 存储专业分数线
all_schools = [44,385,284,542,504,66,293,57,419,530,
               286,291,159,134,97,112,106,133,831,459,
               35,960,240,232,80,170,293,389,287,105,
               84,787,935,342,364,495,367,544,349,36,
               515,100,2995,661,99,178,1009,934,119,512,
               511,96,532,530,533,104,2941,98,391,398,
               961,1029,388,42,439,1026,128,425,428,372,
               62,126,61,499,71,108,157,102,103,63,
               66,68,240,251,109,160,125,132,309,324,
               421,129,379,33,32,262,139,599,41,59,
               60,80,46,73,123,140,31,584,38,52,
               576,143,48,592,91,169,110,86,118
]  # 存储所有学校ID
'''
all_provinces = [11, 12, 13, 14, 15,
                 21, 22, 23, 31, 32,
                 33, 34, 35, 36, 37,
                 41, 42, 43, 44, 45,
                 46, 50, 51, 52, 53,
                 61, 62, 63, 64, 65]  # 所有省份ID
'''
all_provinces = [32,42,43]  # 所有省份ID
all_years = [2024,2023,2022,2021]

MAX_RETRIES = 5  # 最大允许连续失败次数
retry_count = 0   # 当前失败次数
save_lock = threading.Lock()  # 定义锁
global_exit_flag = False  #结束进程标志
exit_lock = threading.Lock()

# 从数据库获取院校id
def get_schools_from_db():
    try:
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor()

        # 执行查询
        cursor.execute("SELECT school_id FROM 所有院校信息")
        school_ids = [row[0] for row in cursor.fetchall()]

        print(f"成功获取{len(school_ids)}条院校ID")
        return school_ids

    except pymysql.Error as e:
        print(f"数据库操作失败：{str(e)}")
        return []
    finally:
        if 'connection' in locals() and connection.open:
            cursor.close()
            connection.close()


#爬取院校分数线
def spider_college(school_id, province_id):
    url = f"https://api.eol.cn/web/api/?local_province_id={province_id}&school_id={school_id}&uri=apidata/api/gk/score/province&year=2024"
    headers = {
        "User-Agent": UserAgent().random
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        items = response.json().get('data', {}).get('item', [])
        #print(items)
        for item in items:
            data1 = {
                'year': item.get('year'),  #年份
                'school_name': item.get('name'),  #院校名称
                'school_id': item.get('school_id'),  #院校id
                'province': item.get('local_province_name'),  #招生省份
                'batch': item.get('local_batch_name'),  #录取批次
                'sp_group': item.get('sg_name'),  #专业组号
                'sub_cat': item.get('local_type_name'),  #选科大类
                'sub_req': item.get('sg_info'),  #选科要求
                'min_score': item.get('min'),  #最低分
                'lowest_rank': item.get('min_section'),  #最低位次
                'proscore': item.get('proscore'),  #省控线
            }
            print(data1)
            college_data.append(data1)
    time.sleep(random.uniform(4 ,8))


#爬取专业分数线
def spider_specialty(school_id, province_id, year):
    global retry_count, global_exit_flag  # 允许函数修改全局变量
    # 每次请求前检查退出标志
    with exit_lock:
        if global_exit_flag:
            return

    base_url = "https://api.eol.cn/web/api/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        "Referer": "https://www.gaokao.cn/",
        "Accept":"application/json, text/plain, */*",
    }
    page = 1  # 从第一页开始爬取
    size = 10  # 每页数据条数

    # 确保不同省份的数据存入各自的列表
    if province_id not in specialty_data_by_province:
        specialty_data_by_province[province_id] = []

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
                with exit_lock:
                    global_exit_flag = True  # 设置全局退出标志
                save_specialty_to_excel()
                save_specialty_to_mysql()
                os._exit(0)  # 终止程序

        except Exception as e:
            print(f"❌ 解析 JSON 失败: {str(e)}")
            retry_count += 1

        # 失败次数超过阈值，自动保存并退出
        if retry_count >= MAX_RETRIES:
            print("⚠️ 失败次数过多，保存数据并退出...")
            with exit_lock:
                global_exit_flag = True
            save_specialty_to_excel()
            save_specialty_to_mysql()
            os._exit(0)

        data = response.json().get('data', {})

        items = data.get('item', [])
        num_found = data.get('numFound', 0)  # 获取总数据条数
        #print(items)
        if not items:
            print(f"✅ 爬取完成，{school_id} 在 {province_id} {year} 年没有更多数据")
            break  # 没有更多数据，退出循环

        for item in items:
            data1=({
                'year': item.get('year'),  #年份
                'school_name': item.get('name'),  #院校名称
                'school_id': item.get('school_id'),  #院校id
                'province': item.get('local_province_name'),  #招生省份
                'batch': item.get('local_batch_name'),  #录取批次
                'sub_cat': item.get('local_type_name'),  #选科大类
                'sub_req': item.get('sg_info'),  #选科要求
                'sp_group': item.get('sg_name'),  #专业组号
                'major': item.get('sp_name'),  #专业名称
                'max_score': item.get('max'),  #最高分
                'min_score': item.get('min'),  #最低分
                'lowest_rank': item.get('min_section'),  #最低位次
            })
            print(data1)
            specialty_data_by_province[province_id].append(data1)
        print(f"✅ 已爬取 {school_id} 在 {province_id} {year} 年的第 {page} 页，共 {len(items)} 条数据")

        # 计算总页数
        total_pages = (num_found // size) + (1 if num_found % size else 0)
        # 如果当前页数达到总页数，停止循环
        if page >= total_pages:
            break

        page += 1  # 进入下一页
        time.sleep(random.uniform(7.6, 9.3))  # 限制请求频率，防止封禁


# 用多线程爬取所有学校和省份数据
def main():
    #all_schools=get_schools_from_db()
    print(f"正在爬取 {len(all_schools)} 所院校的数据...")

    max_workers = 10  # 线程池最大线程数
    threadPool = ThreadPoolExecutor(max_workers)

    #爬取院校专业分数线
    futures = []
    for year in all_years:
        for school_id in all_schools:
            for province_id in all_provinces:
                futures.append(threadPool.submit(spider_specialty, school_id, province_id, year))
                time.sleep(random.uniform(7.6, 9.3))  # 请求间隔

    for future in futures:
        future.result()  # 等待所有任务完成
    threadPool.shutdown()

    save_specialty_to_excel()  # 保存专业分数线数据
    save_specialty_to_mysql()
    print("数据爬取完成！")


#保存专业分数线数据
def save_specialty_to_excel():
    with save_lock:  # 确保只有一个线程执行此函数，保存数据
        base_folder = "各省份专业分数线"
        os.makedirs(base_folder, exist_ok=True)  # 确保目录存在

        for province_id, data in specialty_data_by_province.items():
            df = pd.DataFrame(data)
            if df.empty:
                continue

            province_name = df["province"].iloc[0] if "province" in df.columns and not df.empty else f"省份_{province_id}"
            province_folder = os.path.join(base_folder, f"{province_id}-{province_name}-专业分数线")

            os.makedirs(province_folder, exist_ok=True)

            filename = os.path.join(province_folder, "专业分数线.xlsx")
            df.to_excel(filename, index=False)

            print(f"✅ {province_name} 数据已保存到 {filename}")


def save_specialty_to_mysql():
    with save_lock:
        try:
            engine = create_engine(
                f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}?charset={DB_CONFIG['charset']}"
            )

            for province_id, data in specialty_data_by_province.items():
                df = pd.DataFrame(data)
                if df.empty:
                    continue

                province_name = df["province"].iloc[0] if "province" in df.columns and not df.empty else f"省份_{province_id}"
                table_name = f"{province_name}招生情况"

                # 写入数据库
                df.to_sql(
                    name=table_name,
                    con=engine,
                    if_exists='replace',  # 若需追加改为'append'
                    index=False,
                    dtype={
                        'year': Integer(),
                        'school_name': String(20),
                        'school_id': Integer(),
                        'province': String(20),
                        'batch': String(20),
                        'sub_cat': String(10),
                        'sub_req': String(20),
                        'sq_group': String(20),
                        'major': String(20),
                        'max_score': String(20),
                        'min_score': String(20),
                        'lowest_rank': String(20),
                    }
                )
                print(f"表 {table_name} 写入成功！")

        except Exception as e:
            print(f"数据库写入失败: {str(e)}")




if __name__ == '__main__':
    main()
