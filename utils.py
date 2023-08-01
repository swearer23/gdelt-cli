from datetime import datetime, timedelta

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