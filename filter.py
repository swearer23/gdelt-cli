import os
from multiprocessing import Pool, cpu_count
import pandas as pd
from main import get_date_list, COLS

topics = [
    'WB_678_DIGITAL_GOVERNMENT',
    'WB_2417_DIGITAL_MANUFACTURING',
    'WB_2345_DIGITAL_ECONOMY_STRATEGY',
    'WB_2364_E_COMMERCE_APPLICATIONS',
    'SLFID_ECONOMIC_POWER',
    'WB_665_SOFTWARE_AS_A_SERVICE',
    'WB_2945_DATABASE',
    'TECH_BIGDATA',
    'WB_661_BIG_DATA',
    'WB_667_ICT_INFRASTRUCTURE',
    'WB_652_ICT_APPLICATIONS',
    'TAX_FNCACT_DATABASE_ADMINISTRATOR',
    'WB_133_INFORMATION_AND_COMMUNICATION_TECHNOLOGIES',
    'WB_1018_ELECTRONIC_COMMERCE_LAW',
    'WB_2101_ANTITRUST',
    'WB_2031_RIGHT_TO_INFORMATION',
    'WB_873_NON_TRADITIONAL_DATA_DRIVEN_MANAGEMENT',
    'WB_680_PERSONAL_DATA_PROTECTION',
    'WB_1070_ECONOMIC_GROWTH_POLICY'
]

def init_filtered_csv():
    for topic in topics:
        df = pd.DataFrame(columns=COLS)
        df.to_csv(f'filtered/{topic}.csv', index=False)

def topic_categorizer(df):

    df.dropna(subset=['V2Themes'], inplace=True)

    df_list = [{
        'topic': x,
        'df': df[df['V2Themes'].str.contains(x)]
    } for x in topics]

    for df in df_list:
        filename = f'filtered/{df["topic"]}.csv'
        df['df'].to_csv(filename, mode='a', header=False, index=False)

def filter_location(df):
    df = df.copy()
    df['CH'] = df['V2Locations'].str.contains('#CH')
    df = df[df['CH'] == True]
    df.drop(['CH'], axis=1, inplace=True)
    return df

def main(filename):
    if not os.path.exists(filename):
        print(f'{filename}不存在')
        return
    else:
        print(f'开始处理{filename}')
    df = pd.read_csv(filename)
    df = filter_location(df)
    topic_categorizer(df)

init_filtered_csv()
datelist = get_date_list('20230101', '20230201')
pool = Pool(cpu_count())
pool.map(main, [f'/mnt/d/gdelt-raw/{date}.csv' for date in datelist])

