import requests
from bs4 import BeautifulSoup
import pandas as pd


# 目标网址（中南大学的历年分数页面）
url = "https://zhaosheng.csu.edu.cn/bkzn/lnfs/a2023.htm"

# 发送请求获取网页内容
response = requests.get(url)
response.encoding = 'utf-8'  # 指定编码格式
html_content = response.text

# 使用BeautifulSoup解析网页
soup = BeautifulSoup(html_content, "html.parser")

# 存储数据的列表
data = []

# 查找所有包含录取分数的表格，这里假设表格是以 <table> 标签表示
tables = soup.find_all("table")

# 遍历每个表格并提取信息
for table in tables:
    rows = table.find_all("tr")
    for row in rows:
        cols = row.find_all("td")

        # 只处理列数大于等于6的行
        if len(cols) >= 6:
            province = cols[0].text.strip()
            control_line = cols[1].text.strip()
            admission_line = cols[2].text.strip()
            highest_score = cols[3].text.strip()
            lowest_score = cols[4].text.strip()
            avg_score = cols[5].text.strip()

            # 存储为字典
            data.append({
                "省份": province,
                "控制线": control_line,
                "投档线": admission_line,
                "最高分": highest_score,
                "最低分": lowest_score,
                "平均分": avg_score
            })
        else:
            # 如果该行列数不够，则跳过该行
            continue

# 打印结果
for entry in data:
    print(entry)

# 将数据转换为 DataFrame
df = pd.DataFrame(data)

# 将数据写入Excel文件
df.to_excel("C:\\Users\\22346\\Desktop\\test.xlsx", index=False, engine='openpyxl')