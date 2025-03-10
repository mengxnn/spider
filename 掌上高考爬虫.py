import time
import pandas as pd
import requests
import json
from fake_useragent import UserAgent
import random
from concurrent.futures import ThreadPoolExecutor  # 多线程爬取

# 全局变量：存储院校分数线和专业分数线数据
college_data = {}
specialty_data = {}

# 全局变量：索引计数
index_college = 0
index_specialty = 0


def get_config(page, school_id, data_type="college"):
    """
    构建请求参数、请求头和目标URL
    参数：
        page: 当前页码
        school_id: 学校ID
        data_type: 爬取类型，'college' 代表院校分数线，'specialty' 代表专业分数线
    """
    base_params = {
        "local_province_id": "43",  # 省份ID (43为湖南，41为河南)
        "page": page,
        "school_id": school_id,
        "size": 10,
        "year": 2024,
        "special_group": "",
        "signsafe": "746698ac890bc8948ecf5c0cf9537dd7",  # 安全签名
    }

    if data_type == "college":
        # 院校分数线 API 参数
        base_params.update({
            "uri": "apidata/api/gk/score/province",
        })
    elif data_type == "specialty":
        # 专业分数线 API 参数
        base_params.update({
            "local_batch_id": 14,  # 本科一批
            "local_type_id": "2073",  # 理科
            "uri": "apidata/api/gk/score/special",
        })

    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": UserAgent().random
    }

    url = f"https://api.zjzw.cn/web/api/?" + "&".join(f"{key}={value}" for key, value in base_params.items())

    return base_params, headers, url


def request_api(url, params, headers):
    """ 发送请求并解析JSON数据 """
    try:
        response = requests.post(url, json=params, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"请求失败，状态码：{response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"请求异常：{e}")
        return None


def spider_college(school_id):
    """ 爬取院校分数线数据"""
    global college_data, index_college
    params, headers, url = get_config(1, school_id, "college")
    result = request_api(url, params, headers)
    if not result:
        return

    data_all = result.get('data', {})
    result_numFound = data_all.get('numFound', 0)
    total_pages = (result_numFound // 10) + (1 if result_numFound % 10 else 0)

    for page in range(1, total_pages + 1):
        params, headers, url = get_config(page, school_id, "college")
        result_page = request_api(url, params, headers)
        if result_page:
            items = result_page.get('data', {}).get('item', [])
            get_data_college(college_data, items)
        time.sleep(random.uniform(1, 3))


def spider_specialty(school_id):
    """ 爬取专业分数线数据"""
    global specialty_data, index_specialty
    params, headers, url = get_config(1, school_id, "specialty")
    result = request_api(url, params, headers)
    if not result:
        return

    data_all = result.get('data', {})
    result_numFound = data_all.get('numFound', 0)
    total_pages = (result_numFound // 10) + (1 if result_numFound % 10 else 0)

    for page in range(1, total_pages + 1):
        params, headers, url = get_config(page, school_id, "specialty")
        result_page = request_api(url, params, headers)
        if result_page:
            items = result_page.get('data', {}).get('item', [])
            get_data_specialty(specialty_data, items)
        time.sleep(random.uniform(1, 3))


def get_data_college(data, items):
    """ 解析院校分数线数据 """
    global index_college
    for item in items:
        #print(item)
        data1 = {
            '年份': item.get('year'),
            '院校名称': item.get('name'),
            '招生省份': item.get('local_province_name'),
            '录取批次': item.get('local_batch_name'),
            '招生类型': item.get('local_type_name'),
            '专业组': item.get('sg_name'),
            '选科要求': item.get('sg_info'),
            '最低分/最低位次': str(item.get('min')) + '/' + str(item.get('min_section')),
            '省控线': item.get('proscore'),
        }
        data[index_college] = data1
        print(data1)
        index_college += 1


def get_data_specialty(data, items):
    """ 解析专业分数线数据 """
    global index_specialty
    for item in items:
        #print(item)
        data1 = {
            '年份': item.get('year'),
            '院校名称': item.get('name'),
            '招生省份': item.get('local_province_name'),
            '录取批次': item.get('local_batch_name'),
            '选科要求': item.get('sg_info'),
            '专业名称': item.get('level3_name'),
            '最高分': item.get('max'),
            '最低分/最低位次': str(item.get('min')) + '/' + str(item.get('min_section')),
        }
        data[index_specialty] = data1
        print(data1)
        index_specialty += 1


def main():
    """ 主函数，爬取院校分数线和专业分数线 """
    max_workers = 10
    futures = []
    start_time = time.time()
    threadPool = ThreadPoolExecutor(max_workers)

    school_id = 459  # 目标学校ID

    # 爬取院校分数线
    futures.append(threadPool.submit(spider_college, school_id))

    # 爬取专业分数线
    futures.append(threadPool.submit(spider_specialty, school_id))

    for future in futures:
        future.result()

    threadPool.shutdown()

    # 保存数据到JSON和Excel
    save_data(college_data, "院校分数线")
    save_data(specialty_data, "专业分数线")

    print("总耗时：", time.time() - start_time)


def save_data(data, filename):
    """ 保存数据到 JSON 和 Excel """
    with open(f"{filename}.json", "w", encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)

    df = pd.DataFrame.from_dict(data, orient='index')
    df.to_excel(f"{filename}.xlsx", index=False)
    print(f"{filename} 数据已保存")


if __name__ == '__main__':
    main()
