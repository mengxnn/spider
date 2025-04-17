import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, String  # 新增导入类型
import re
import os

# pip install pymysql sqlalchemy

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',
    'database': 'yfydb',
    'charset': 'utf8mb4'
}

types = [6457, 6458, 6461, 6462, 6486, 6487]  # 湖南、湖北、江苏
data=[]

# 爬取并保存一分一段表数据
def crawl_score_table(url):
    # 设置合法请求头
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Referer": "http://www.gkzy.com/"
    }

    try:
        # 发送HTTP请求
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'utf-8'
        # 解析HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', border="1", cellpadding="0")

        # 表头
        headers = ['year', 'province', 'subject1', 'score', 'num', 'tot_num']
        # 提取数据行
        rows = []
        for tr in table.find('tbody').find_all('tr'):
            row = [td.text.strip() for td in tr.find_all('td')]
            if(row[0]=='省份' or row[0]=='年份'):  # 去除表头
                continue
            rows.append(row)

        # 转换为DataFrame
        df = pd.DataFrame(rows, columns=headers)
        return df

    except Exception as e:
        print(f"爬取失败: {str(e)}")
        return None

# 清洗分数数据，返回可存储的规范格式
def clean_score(score_str):
    try:
        # 处理 "665（含以上）" 类数据 -> 提取数字部分
        if "（含以上）" in score_str or "(含以上)" in score_str:
            return int(re.search(r'\d+', score_str).group())

        # 处理 "100以下" 类数据 -> 转换为0
        elif "以下" in score_str:
            return 0

        # 普通数字直接转换
        else:
            return int(score_str)
    except:
        return None  # 无法处理的格式返回None

# 将DataFrame保存到MySQL数据库
def save_to_mysql(df):
    try:
        # 数据清洗

        df['score'] = df['score'].apply(clean_score)
        # 移除无效数据
        df = df.dropna(subset=['score'])

        # 创建数据库连接引擎
        engine = create_engine(
            f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}?charset={DB_CONFIG['charset']}"
        )

        # 按省份和科目分组
        grouped = df.groupby(['province', 'subject1'])
        for (province, subject), group_df in grouped:
            # 生成规范表名（去除特殊字符）
            table_name = re.sub(
                r'[^\w]', '',
                f"{province}{subject}一分一段表"
            )

            # 写入数据库
            group_df.to_sql(
                name=table_name,
                con=engine,
                if_exists='replace',  # 若需追加改为'append'
                index=False,
                dtype={
                    'year': Integer(),
                    'province': String(20),
                    'subject1': String(10),
                    'score': Integer(),
                    'num': Integer(),
                    'tot_num': Integer()
                }
            )
            print(f"表 {table_name} 写入成功！")

    except Exception as e:
        print(f"数据库写入失败: {str(e)}")


if __name__ == "__main__":
    for type in types:
        target_url = f"http://www.gkzy.com/zytb/score_search/yfydb/detail/{type}.html"
        print(f"正在爬取：{target_url}")
        data.append(crawl_score_table(target_url))
        time.sleep(1.2) # 限制请求频率，防止封禁

    if data:
        df = pd.concat(data)
        # 按省份和科目保存Excel
        for (province, subject), group_df in df.groupby(['province', 'subject1']):
            # 处理特殊字符
            safe_province = province.replace("/", "-").replace(" ", "")
            safe_subject = subject.replace("/", "-").replace(" ", "")

            # 创建省份文件夹
            province_dir = os.path.join("一分一段表", safe_province)
            os.makedirs(province_dir, exist_ok=True)

            # 保存文件
            file_path = os.path.join(province_dir, f"{safe_subject}.xlsx")
            group_df.to_excel(file_path, index=False)
            print(f"文件已保存至：{file_path}")

        # 保存到数据库
        save_to_mysql(df)
        print(f"共处理{len(data)}个数据表")
    else:
        print("未能获取数据")