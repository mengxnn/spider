import time
import pandas as pd
import requests
import json
from fake_useragent import UserAgent
import random
from concurrent.futures import ThreadPoolExecutor  # 用于实现多线程爬取

# 全局变量：用于存储所有爬取的数据（以字典形式存储，key为索引）
temp = {}
# 全局变量：用于记录数据的索引
index = 0


def get_config(page, school_id):
    """
    根据当前页码和学校ID构建请求参数、请求头和目标URL。

    参数:
        page: 当前请求的页码
        school_id: 学校的ID（如459代表某个学校）

    返回:
        params: 请求参数字典
        headers: 请求头字典（随机User-Agent）
        url: 请求的完整URL（包含部分参数，通过format拼接页码和学校ID）
    """
    params = {
        "local_batch_id": 7,  #本科一批
        "local_province_id": "44",  #在该省份的招生情况，41为河南，44为湖南
        "local_type_id": "2",  # 理科
        "page": page,
        "school_id": school_id,
        "signsafe": "bbddec69cb7fe50f4d9ea21404d72fcf",
        "size": 10,  # 每页返回的数据条数
        "special_group": "",
        "uri": "apidata/api/gkv3/plan/school",
        "year": 2023
    }
    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": UserAgent().random  # 随机生成User-Agent，模拟真实请求
    }
    # 网站请求头header，在network中搜local可得
    url = "https://api.zjzw.cn/web/api/?like_spname=&local_batch_id=14&local_province_id=44&local_type_id=2073&page=1&school_id=459&sg_xuanke=&size=10&special_group=&uri=apidata/api/gkv3/plan/school&year=2024&signsafe=ac9335057ce8c0896d15130cdfd210d8".format(
        page, school_id)
    return params, headers, url


def request_api(url, params, headers):
    """
    发起POST请求，并返回响应的JSON数据。

    参数:
        url: 请求的URL
        params: 请求体中的参数（以JSON格式发送）
        headers: 请求头

    返回:
        如果请求成功，返回解析后的JSON字典；否则返回None。
    """
    try:
        # 发起POST请求，不使用代理（之前代理请求部分已去除）
        response = requests.post(url, json=params, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"请求失败，状态码：{response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"请求出现异常：{e}")
        return None


def spider(school_id):
    """
    针对指定学校ID进行爬取：
    1. 先获取第一页数据，确定总记录数（numFound字段）。
    2. 根据总记录数计算总页数，并逐页请求数据。
    3. 将每页数据交给get_data函数进行处理和存储。

    参数:
        school_id: 学校的ID（例如459）
    """
    global temp, index
    # 请求第一页数据
    params, headers, url = get_config(1, school_id)
    result = request_api(url, params, headers)
    if not result:
        return
    # 从返回数据中提取总记录数（numFound字段）
    data_all = result.get('data', {})
    result_numFound = data_all.get('numFound', 0)
    # 计算总页数，每页10条数据
    total_pages = int(result_numFound / 10) + (1 if result_numFound % 10 else 0)
    page_list = [page for page in range(1, total_pages + 1)]

    for page in page_list:
        # 构建每一页的请求参数
        params, headers, url = get_config(page, school_id)
        result_page = request_api(url, params, headers)
        if result_page:
            # 从返回数据中提取“item”列表，即具体的专业数据
            items = result_page.get('data', {}).get('item', [])
            get_data(temp, items)  # 调用数据处理函数
        time.sleep(random.uniform(1, 3))  # 随机休眠1-3秒，防止访问过快


def get_data(data, items):
    """
    解析每一页返回的专业数据，将其中需要的字段提取出来，并存储到全局字典中。

    参数:
        data: 全局数据存储字典
        items: 当前页面返回的专业数据列表，每个元素为一个字典
    """
    global index
    for item in items:
        # 构造存储每个专业数据的字典
        data1 = {}
        data1['name'] = item.get('name')  # 专业名称
        data1['province_name'] = item.get('province_name')  # 省份名称
        data1['num'] = item.get('num')  # 招生计划数
        data1['spname'] = item.get('spname')  # 专业名称缩写或其他描述
        data1['spcode'] = item.get('spcode')  # 专业代码
        data1['length'] = item.get('length')  # 学制或其他相关信息
        data1['level2_name'] = item.get('level2_name')  # 专业类别（二级名称）
        data1['local_batch_name'] = item.get('local_batch_name')  # 批次名称（如本科一批、二批等）
        data1['local_type_name'] = item.get('local_type_name')  # 类型名称，如文理分科等
        data[index] = data1  # 将处理后的数据存入全局字典
        print(data1)  # 输出当前专业数据以便调试
        index += 1


def main():
    """
    主函数：
    1. 使用线程池多线程爬取数据（这里只爬取指定学校的专业数据）。
    2. 等待所有线程执行完毕后，将数据保存为JSON，再转换为Excel文件。
    """
    max_workers = 10  # 最大线程数
    futures = []
    start_time = time.time()
    threadPool = ThreadPoolExecutor(max_workers)

    # 这里只爬取学校ID为459的数据（可以根据需要添加其他学校ID）
    for school_id in [459]:
        future = threadPool.submit(spider, school_id)
        futures.append(future)

    # 等待所有线程任务完成
    for future in futures:
        future.result()  # 阻塞等待任务结束

    threadPool.shutdown()

    # 将数据保存为JSON文件，再利用pandas转换为Excel
    path = "河南大学"  # 文件命名（可根据实际学校名称修改）
    with open(path + ".json", "w", encoding='utf-8') as f:
        json.dump(temp, f, ensure_ascii=False)

    # 从JSON读取数据并转换为DataFrame，然后导出为Excel
    df = pd.read_json(path + ".json", orient='index')
    df.to_excel(path + ".xlsx", index=False)
    print("爬取耗时：", time.time() - start_time)


if __name__ == '__main__':
    main()
