import os
from datetime import datetime

import pandas as pd


def read_csv_safely(path):
    for enc in ["utf-8", "cp1251", "windows-1251", "utf-8-sig"]:
        try:
            return pd.read_csv(path, dtype=str, encoding=enc, sep=";", engine="python")
        except Exception:
            continue
    raise ValueError(f"Не удалось прочитать файл: {path}")


sbis_folder = "Входящие"
sbis_list = []

for file in os.listdir(sbis_folder):
    if file.lower().endswith(".csv"):
        path = os.path.join(sbis_folder, file)
        df = read_csv_safely(path)
        sbis_list.append(df)

if not sbis_list:
    raise ValueError("В папке Входящие нет CSV файлов")

sbis_df = pd.concat(sbis_list, ignore_index=True)

new_columns = [
    "Дата", "Номер", "Сумма", "Статус", "Примечание", "Комментарий",
    "Контрагент", "ИНН_КПП", "Организация", "ИНН_КПП_1", "Тип_документа",
    "Имя_файла", "Дата_1", "Номер_1", "Сумма_1", "Сумма_НДС",
    "Ответственный", "Подразделение", "Код", "Дата_2", "Время",
    "Тип_пакета", "Идентификатор_пакета", "Запущено_в_обработку",
    "Получено_контрагентом", "Завершено", "Увеличение_суммы",
    "НДС", "Уменьшение_суммы", "НДС_1"
]
sbis_df.columns = [col.replace(" ", "_") for col in new_columns]

valid_doc_types = ["СчФктр", "УпдДоп", "УпдСчфДоп", "ЭДОНакл"]
sbis_filtered = sbis_df[sbis_df["Тип_документа"].isin(valid_doc_types)].drop_duplicates(subset=["Номер"], keep="first")

sbis_filtered = sbis_filtered.rename(columns={"Дата": "Дата_сбис"})

apteka_folder = "Аптеки/csv/correct/"
today_str = datetime.today().strftime("%Y-%m-%d")
result_folder = os.path.join("Результат", today_str)
os.makedirs(result_folder, exist_ok=True)

for file in os.listdir(apteka_folder):
    if not file.lower().endswith(".csv"):
        continue

    path = os.path.join(apteka_folder, file)
    apteka_df = read_csv_safely(path)

    apteka_df["Номер_счет-фактуры"] = ""
    apteka_df["Сумма_счет-фактуры"] = ""
    apteka_df["Дата_счет-фактуры"] = ""
    apteka_df["Сравнение_дат"] = ""

    mask = apteka_df["Поставщик"] == "ЕАПТЕКА"
    apteka_df.loc[mask, "Номер накладной"] = (
            apteka_df.loc[mask, "Номер накладной"].astype(str) + "/15"
    )

    sbis_filtered_for_merge = sbis_filtered[["Номер", "Сумма", "Дата_сбис"]].drop_duplicates(subset=["Номер"],
                                                                                             keep="first")
    merged = apteka_df.merge(
        sbis_filtered_for_merge,
        left_on="Номер накладной",
        right_on="Номер",
        how="left",
    )

    apteka_df["Номер_счет-фактуры"] = merged["Номер"].values
    apteka_df["Сумма_счет-фактуры"] = merged["Сумма"].values

    dt_parsed = pd.to_datetime(
        merged["Дата_сбис"],
        format="%d.%m.%Y",
        errors="coerce"
    )
    apteka_df["Дата_счет-фактуры"] = dt_parsed.dt.strftime("%d.%m.%Y")

    date_invoice = pd.to_datetime(apteka_df["Дата накладной"].str.strip(), errors="coerce", dayfirst=True)
    date_facture = dt_parsed
    mask_mismatch = (
            (date_invoice != date_facture) |
            (date_invoice.isna() ^ date_facture.isna())
    )
    apteka_df["Сравнение_дат"] = ""
    apteka_df.loc[mask_mismatch, "Сравнение_дат"] = "Не совпадает!"

    final_columns = [
        '№ п/п', 'Штрих-код партии', 'Наименование товара', 'Поставщик',
        'Дата приходного документа', 'Номер приходного документа',
        'Дата накладной', 'Номер накладной', 'Номер_счет-фактуры',
        'Сумма_счет-фактуры', 'Кол-во',
        'Сумма в закупочных ценах без НДС', 'Ставка НДС поставщика',
        'Сумма НДС', 'Сумма в закупочных ценах с НДС',
        'Дата_счет-фактуры', 'Сравнение_дат'
    ]
    for col in final_columns:
        if col not in apteka_df.columns:
            apteka_df[col] = ""

    base_filename = os.path.splitext(file)[0]
    output_path = os.path.join(result_folder, f"{base_filename} - результат.xlsx")
    apteka_df[final_columns].to_excel(output_path, index=False, engine="openpyxl")

    print(f"Файл для аптеки {file} сохранён: {output_path}")

print("Готово ✅ Все файлы аптек сохранены по отдельности.")
