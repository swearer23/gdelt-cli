from datetime import datetime, timedelta
from io import BytesIO
import time
import requests
import pandas as pd
from tqdm import tqdm

COLS = ['GKGRECORDID', 'DATE', 'SourceCollectionIdentifier', 'SourceCommonName', 'DocumentIdentifier', 'Counts', 'V2Counts', 'Themes', 'V2Themes', 'Locations', 'V2Locations', 'Persons', 'V2Persons', 'Organizations', 'V2Organizations', 'V2Tone', 'Dates', 'GCAM', 'SharingImage', 'RelatedImages', 'SocialImageEmbeds', 'SocialVideoEmbeds', 'Quotations', 'AllNames', 'Amounts', 'TranslationInfo', 'Extras']
START_DATE = '20240101'
END_DATE = '20240110'
topics = [
  'TECH_AUTOMATION',
  'WB_1227_AUTOMOTIVE_VALUE_CHAIN',
  'TAX_FNCACT_AUTOMECHANICS',
  'TAX_FNCACT_AUTOMECHANIC',
  'WB_1767_ENERGY_FINANCE',
  'TAX_FNCACT_AUTOMOBILE_DETAILER',
  'WB_775_TRADE_POLICY_AND_INTEGRATION',
  'WB_2575_TRADE_POLICY_AND_INVESTMENT_AGREEMENTS',
  'ECON_TRADE_DISPUTE',
  'WB_776_TRADE_POLICY'
]

def init_filtered_csv():
  for topic in topics:
    df = pd.DataFrame(columns=COLS)
    df.to_csv(f'.data/{topic}.csv', index=False)
  topic_file_map = {topic: f'.data/{topic}.csv' for topic in topics}
  return topic_file_map

def download_data(url: str) -> pd.DataFrame:
  error = None
  for _ in range(5):
    try:
      r = requests.get(url)
      r.raise_for_status()
      return r.content
    except Exception as e:
      error = e
      time.sleep(5)
  if error:
    print(error)
  return None

def get_date_list(start_date, end_date) -> list[str]:
  date_list = []
  begin_date = datetime.strptime(start_date, "%Y%m%d")
  end_date = datetime.strptime(end_date, "%Y%m%d")
  while begin_date < end_date:
    date_str = begin_date.strftime("%Y%m%d%H%M%S")
    date_list.append(date_str)
    begin_date += timedelta(minutes=15)
  return date_list

def filter_location(df):
  df = df.copy()
  df['CH'] = df['V2Locations'].str.contains('#CH')
  df = df[df['CH'] == True]
  df.drop(['CH'], axis=1, inplace=True)
  return df

def topic_categorizer(df, topic_file_map: dict[str, str]):
  records = df.to_dict(orient='records')
  for record in records:
    for topic in topics:
      if record['V2Themes'].find(topic) != -1:
        df = pd.DataFrame([record])
        df.to_csv(topic_file_map[topic], mode='a', header=False, index=False)

def process(content, topic_file_map: dict[str, str]):
  buffer = BytesIO(content)
  frame = pd.read_csv(buffer, compression='zip', sep='\t', encoding='latin-1')  # ,
  # parse_dates=[1, 2])
  frame.columns = COLS
  buffer.flush()
  buffer.close()
  df = filter_location(frame)
  df.dropna(subset=['V2Themes'], inplace=True)
  topic_categorizer(df, topic_file_map)

def main():
  datelist = get_date_list(START_DATE, END_DATE)
  urllist = [f'http://data.gdeltproject.org/gdeltv2/{date}.gkg.csv.zip' for date in datelist]
  topic_file_map = init_filtered_csv()
  for url in tqdm(urllist, desc='Progress'):
    data = download_data(url)
    if data:
      process(data, topic_file_map)

if __name__ == '__main__':
  main()