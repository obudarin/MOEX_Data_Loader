# Этот код предназначен для загрузки исторических данных по акциям с Московской биржи (MOEX) 
# для заданного списка ценных бумаг и временного интервала. 
# Данные загружаются с использованием API MOEX и обрабатываются для получения структурированного 
# набора данных, который затем сохраняется в файл CSV.

import requests
import pandas as pd

# Задаем параметры запроса: список ценных бумаг, даты начала и конца периода, интервал и длину периода
securities_list = ["LQDT", "TGLD", "TRUR", "AKMM", "SBMX", "TMOS", "EQMX", "GOLD", "SBMM", "AKME", "AKMB", "RCMX", "AMRE", "AKGD", "SBHI", "OPNR", "DIVD", "MKBD", "SBRI", "GROD"]  
start_date = "2024-01-01"
end_date = "2024-12-31"
interval = "1day"
period_length = 100
max_securities_per_request = 1  

# Определяем функцию для получения данных за один период
def get_data_for_period(security, period_start, period_end):
    # Формируем URL для запроса данных
    url = f"https://iss.moex.com/iss/history/engines/stock/markets/shares/boards/TQTF/securities/{security}.json?from={period_start}&till={period_end}&interval={interval}"
    # Отправляем запрос и проверяем статус ответа
    response = requests.get(url)
    if response.status_code == 200:
        # Если ответ успешный, преобразуем данные в DataFrame и добавляем столбец с тикером
        data = pd.DataFrame(response.json()["history"]["data"])
        data["SECID"] = security
        return data
    else:
        # Если возникла ошибка, выводим сообщение об ошибке
        print(f"Ошибка при получении данных за период {period_start} - {period_end}: {response.status_code}")
        return None

# Разделяем диапазон дат на периоды
periods = []
current_date = pd.to_datetime(start_date)
while current_date <= pd.to_datetime(end_date):
    next_date = current_date + pd.Timedelta(days=period_length - 1)
    periods.append((current_date.strftime("%Y-%m-%d"), next_date.strftime("%Y-%m-%d")))
    current_date = next_date + pd.Timedelta(days=1)

# Запускаем циклы по инструментам и периодам для загрузки данных
all_data = []
for security in securities_list:
    for period_start, period_end in periods:
        data = get_data_for_period(security, period_start, period_end)
        if data is not None:
            all_data.append(data)

# Объединяем данные из всех периодов и всех инструментов
combined_data = pd.concat(all_data, ignore_index=True)

# Выбираем нужные столбцы (дата, тикер, цена закрытия)
processed_data = combined_data[[1, 3, 11]]

# Преобразуем данные в нужный формат
result = processed_data.pivot(index=1, columns=3, values=11)
result.index.name = "DATE"
result = result.reset_index()
result.columns.name = None

# Сохраняем результат в файл CSV
result.to_csv("close_data2.csv", sep=";", index=False)

print("Данные сохранены в файл close_data.csv")