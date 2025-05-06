import time
import pandas as pd
import requests
import random
import sys
from sqlalchemy.types import Integer, String  # 新增导入类型

from sqlalchemy import create_engine

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',
    'database': 'score',
    'charset': 'utf8mb4'
}

# 全局变量
college_info = []  # 存储院校信息
MAX_RETRIES = 5  # 最大允许连续失败次数
retry_count = 0   # 当前失败次数

#爬取院校信息
def spider_college_info():
    global retry_count  # 允许函数修改全局变量
    headers = {  # 添加header字段，模拟用户登陆，防止被封ip
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        "Referer": "https://www.gaokao.cn/",
        "Accept": "application/json, text/plain, */*"
    }
    page = 1  # 从第一页开始爬取
    size = 20  # 每页数据条数

    while 1:
        url = f"https://api.zjzw.cn/web/api/?keyword=&page={page}&province_id=&ranktype=&request_type=1&size=20&top_school_id=[3238,3269]&type=&uri=apidata/api/gkv3/school/lists"
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f"⚠️ 请求失败: {response.status_code}")
            retry_count += 1
        else:
            retry_count = 0  # 成功请求后重置失败计数

        # 处理访问过于频繁的情况
        try:
            data = response.json()
            # print(data)  {'code': '1069', 'message': '访问太过频繁，请稍后再试', 'data': '222.240.53.107', 'location': '', 'encrydata': ''}
            if data.get("code") == "1069":
                print("⚠️ 访问过于频繁，正在保存数据并退出...")
                save_info_to_excel()
                sys.exit(0)  # 终止程序

        except Exception as e:
            print(f"❌ 解析 JSON 失败: {str(e)}")
            retry_count += 1

        # 失败次数超过阈值，自动保存并退出
        if retry_count >= MAX_RETRIES:
            print("⚠️ 失败次数过多，保存数据并退出...")
            save_info_to_excel()
            sys.exit(0)

        if response.status_code == 200:
            data = response.json().get('data', {})
            items = data.get('item', [])
            num_found = data.get('numFound', 0)  # 获取总数据条数
            #print(items)

            for school in items:
                school_info = {
                    'school_name': school.get('name'),
                    'province': school.get('province_name'), # 所在省份
                    'city': school.get('city_name'), # 所在城市
                    'town': school.get('town_name'), # 所在区
                    'belong': school.get('belong'), # 教育行政主管部门
                    'level': school.get('level_name'),  # 本科/专科
                    'type': school.get('type_name'), # 院校类型
                    'nature': school.get('nature_name'), # 办学类型
                    'is985': '是' if school.get('f985', 0) == 1 else '否',
                    'is211': '是' if school.get('f211', 0) == 1 else '否',
                    'isdoubleFC': '是' if school.get('dual_class_name') == "双一流" else '否'
                }
                print(school_info)
                college_info.append(school_info)
            print(f"✅ 已爬取第 {page} 页，共 {len(items)} 条数据")
        else:
            print(f"⚠️ 请求失败: {response.status_code}")

        total_pages = (num_found // size) + (1 if num_found % size else 0)

        # 如果当前页数达到总页数，停止循环
        if page >= total_pages:
            break
        page += 1  # 进入下一页
        time.sleep(random.uniform(2, 4))  # 限制请求频率，防止封禁


def main():
    spider_college_info()
    print("数据爬取完成！")
    save_info_to_excel()  # 保存院校信息至excel表
    save_info_to_mysql()  # 保存院校信息至mysql

def save_info_to_excel():
    df_college_info = pd.DataFrame(college_info)
    df_college_info.to_excel("所有院校信息.xlsx", index=False)
    print("✅ 已成功保存院校信息到 '所有院校信息.xlsx'")

def save_info_to_mysql():
    try:
        engine = create_engine(
            f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}?charset={DB_CONFIG['charset']}"
        )
        df = pd.DataFrame(college_info)
        table_name = "所有院校信息"
        # 写入数据库
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',  # 若需追加改为'append'
            index=False,
            dtype={
                'school_name': String(50),
                'province': String(50),
                'city': String(50),
                'town': String(50),
                'belong': String(50),
                'level': String(50),
                'type': String(50),
                'nature': String(50),
                'is985': String(5),
                'is211': String(5),
                'isdoubleFC': String(5),
            }
        )
        print(f"表 {table_name} 写入成功！")

    except Exception as e:
        print(f"数据库写入失败: {str(e)}")


if __name__ == '__main__':
    main()
