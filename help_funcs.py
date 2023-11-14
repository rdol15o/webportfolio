from datetime import datetime, timedelta

def get_date_intervals(str_date_from, str_date_to):
    '''разбивает интервал дат, возвращает список:
    первый и последний элемент - входящие даты, остальные - первые числа промежуточных месяцев
    '''
    if str_date_from == str_date_to:
        return [str_date_from, str_date_to]

    date_from = datetime.strptime(str_date_from, '%Y-%m-%d')
    date_to = datetime.strptime(str_date_to, '%Y-%m-%d')
    if date_from > date_to:
        date_from, date_to = date_to, date_from

    if date_from.replace(day=1) == date_to.replace(day=1):
        return [date_from.strftime('%Y-%m-%d'), date_to.strftime('%Y-%m-%d')]

    dates = []
    date = date_from
    last_date = date_to.replace(day=1)
    while date <= last_date:
        dates.append(date.strftime('%Y-%m-%d'))
        date = (date.replace(day=1) + timedelta(days=32)).replace(day=1)

    if last_date < date_to:
        dates.append(date_to.strftime('%Y-%m-%d'))

    return dates