import os
from datetime import datetime, timedelta
import time
import warnings
import re
from io import BytesIO
import requests
import pandas as pd

COLS = ['GKGRECORDID', 'DATE', 'SourceCollectionIdentifier', 'SourceCommonName', 'DocumentIdentifier', 'Counts', 'V2Counts', 'Themes', 'V2Themes', 'Locations', 'V2Locations', 'Persons', 'V2Persons', 'Organizations', 'V2Organizations', 'V2Tone', 'Dates', 'GCAM', 'SharingImage', 'RelatedImages', 'SocialImageEmbeds', 'SocialVideoEmbeds', 'Quotations', 'AllNames', 'Amounts', 'TranslationInfo', 'Extras']

# 获取从开始时间到结束时间，每隔15分钟的所有时间列表
def get_date_list(start_date, end_date):
    date_list = []
    begin_date = datetime.strptime(start_date, "%Y%m%d")
    end_date = datetime.strptime(end_date, "%Y%m%d")
    while begin_date < end_date:
        date_str = begin_date.strftime("%Y%m%d%H%M%S")
        date_list.append(date_str)
        begin_date += timedelta(minutes=15)
    return date_list


# 根据日期列表，生成对应的url列表 url 格式为 http://data.gdeltproject.org/gkg/20230730.gkg.csv.zip

def get_url_list(date_list):
    url_list = []
    for date in date_list:
        url = f'http://data.gdeltproject.org/gdeltv2/{date}.gkg.csv.zip'
        url_list.append(url)
    return url_list

def retry_get(url, retry=5):
    if retry == 0:
        return None
    try:
        r = requests.get(url, timeout=5)
        print(r)
        return r
    except requests.exceptions.ConnectionError as e:
        print(e)
        time.sleep(5)
        return retry_get(url, retry - 1)
    except requests.exceptions.ReadTimeout as e:
        print(e)
        time.sleep(5)
        return retry_get(url, retry - 1)
    except Exception as e:
        raise e
    
def check_if_exists(url):
    path = os.path.split(os.path.abspath(__file__))[0] + os.sep  # 获取当前文件所在目录
    data_folder = path + 'raw' + os.sep
    filename = re.search('[0-9]{4,18}', url).group()
    filepath = os.path.join(data_folder, filename + '.csv')
    # 检查URL对应的文件是否已经存储在本地
    if os.path.exists(filepath) or os.path.exists(filepath[:-4]):  # 若文件已下载，则跳过
        print('%s已存在' % filename)
        return True
    else:
        return False

# 下载对应的url列表的数据，并保存到本地
def download_data(url):
    if check_if_exists(url):
        return
    print(url)
    r = retry_get(url)
    if r is None:
        return
    if r.status_code == 404:
        message = "GDELT does not have a url for date time " \
                  "{0}".format(re.search('[0-9]{4,18}', url).group())
        warnings.warn(message)
        return
    buffer = BytesIO(r.content)
    frame = pd.read_csv(buffer, compression='zip', sep='\t', encoding='latin-1')  # ,
    # parse_dates=[1, 2])
    frame.columns = COLS
    print(frame['V2Locations'])
    buffer.flush()
    buffer.close()
    frame.to_csv(f'/mnt/d/gdelt-raw/{re.search("[0-9]{4,18}", url).group()}.csv')

if __name__ == '__main__':
  datelist = get_date_list('20211212', '20230613')
  urllist = get_url_list(datelist)
  # pool = Pool(cpu_count())
  # pool.map(download_data, urllist)
  [download_data(x) for x in urllist]

  # df = pd.read_csv('GDELT_2.0_gdeltKnowledgeGraph_Column_Labels_Header_Row_Sep2016.tsv', sep='\t')
  # print(df['tableId'].to_list())
