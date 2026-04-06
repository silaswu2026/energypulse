"""中国节假日工具 - 判断A股是否休市"""
from datetime import date, timedelta

HOLIDAYS_2025 = {
    (1, 1), (1, 28), (1, 29), (1, 30), (1, 31),
    (2, 1), (2, 2), (2, 3), (2, 4),
    (4, 4), (4, 5), (4, 6),
    (5, 1), (5, 2), (5, 3), (5, 4), (5, 5),
    (5, 31), (6, 1), (6, 2),
    (10, 1), (10, 2), (10, 3), (10, 4),
    (10, 5), (10, 6), (10, 7), (10, 8),
}

HOLIDAYS_2026 = {
    (1, 1), (1, 2), (1, 3),
    (2, 17), (2, 18), (2, 19), (2, 20),
    (2, 21), (2, 22), (2, 23), (2, 24),
    (4, 4), (4, 5), (4, 6),
    (5, 1), (5, 2), (5, 3), (5, 4), (5, 5),
    (6, 19), (6, 20), (6, 21), (6, 22),
    (10, 1), (10, 2), (10, 3), (10, 4),
    (10, 5), (10, 6), (10, 7), (10, 8),
}

WEEKEND_DAYS = [5, 6]


def is_cn_market_open(check_date=None):
    if check_date is None:
        check_date = date.today()
    
    if check_date.weekday() in WEEKEND_DAYS:
        names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
        return False, "周末休市 (" + names[check_date.weekday()] + ")"
    
    month_day = (check_date.month, check_date.day)
    year = check_date.year
    
    if year == 2025 and month_day in HOLIDAYS_2025:
        return False, _get_holiday_name(month_day)
    if year == 2026 and month_day in HOLIDAYS_2026:
        return False, _get_holiday_name(month_day)
    
    return True, ""


def _get_holiday_name(month_day):
    names = {
        (1, 1): "元旦", (1, 28): "春节", (4, 4): "清明节",
        (5, 1): "劳动节", (5, 31): "端午节", (10, 1): "国庆节",
    }
    for k, v in names.items():
        if month_day[0] == k[0] and abs(month_day[1] - k[1]) <= 3:
            return v
    return "节假日"


def get_last_trading_day(check_date=None):
    if check_date is None:
        check_date = date.today()
    prev_day = check_date - timedelta(days=1)
    while not is_cn_market_open(prev_day)[0]:
        prev_day -= timedelta(days=1)
        if (check_date - prev_day).days > 30:
            break
    return prev_day


def get_next_trading_day(check_date=None):
    if check_date is None:
        check_date = date.today()
    next_day = check_date + timedelta(days=1)
    while not is_cn_market_open(next_day)[0]:
        next_day += timedelta(days=1)
        if (next_day - check_date).days > 30:
            break
    return next_day


if __name__ == "__main__":
    today = date.today()
    is_open, reason = is_cn_market_open(today)
    print("今日 (" + str(today) + "): " + ("开市" if is_open else "休市") + " - " + reason)
    if not is_open:
        print("最近交易日: " + str(get_last_trading_day(today)))
