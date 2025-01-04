"""
Integration tables
Author: Arthur Eremenko
About: A utility for downloading and processing data from an excel file into database tables on remote servers
P.S. Please do not judge strictly because the utility was created by me without knowledge
of the python programming language.
I needed a quick solution, and that's why I compiled this program from the libraries.
You have the right to refine and/or improve it.
"""
import datetime
import tkinter as tk
import pandas as pd
import pypyodbc as odbc
from tkinter import *
from tkinter import filedialog
from tkinter import ttk
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import URL

driver_name = '{SQL Server}'
server_name = '172.25.241.20'  # default server ip address
database_name = 'database_name'  # DB name
user_name = 'user_name'  # user name
user_password = 'user_password'  # user pass
connection_url = ''  # connection url
engine = ''
count_records = 0

in_costs_optima_field_list = ['Наименование',
                              'Номер центра затрат',
                              'Статус',
                              'Дата активации',
                              'Дата окончания срока действия',
                              'Код организации']

in_Item_OptiMa_field_list = ['Код',
                             'Название на англ.яз.',
                             'Категория',
                             'Товарная группа',
                             'Код товарной группы',
                             'Название на рус.яз. ',
                             'Единица изм.(код)',
                             'Единица изм.',
                             'Кост центр',
                             'Закупочная категория ТМЦ, 1 сегмент',
                             'Закупочная категория ТМЦ, 2 сегмент',
                             'Закупочная категория ТМЦ, 3 сегмент',
                             'Идентификатор ТМЦ в ERP',
                             'Закупочная категория ТМЦ, все сегменты',
                             'Код организации в ERP',
                             'Наименование организации в ERP',
                             'Код организации']

IN_ITEM_COSTS_OPTIMA_field_list = ['Код организации',
                                   'Код филиала',
                                   'Код склада',
                                   'Код ТМЦ',
                                   'Единица изм.(код)',
                                   'Стоимость',
                                   'Дата стоимости']

in_Supplier_OptiMa_field_list = ['Код',
                                 'Имя/название',
                                 'Основной адрес',
                                 'Страна',
                                 'ИНН',
                                 'Описание',
                                 'Код организации']

root = tk.Tk()
root.geometry("350x280")
root.resizable(width=False, height=False)
root.title('Интеграционные таблицы')

x = (root.winfo_screenwidth() - root.winfo_reqwidth()) / 2
y = (root.winfo_screenheight() - root.winfo_reqheight()) / 2
root.wm_geometry("+%d+%d" % (x, y))

root_styling = ttk.Style()
root_styling.theme_use('clam')

lf_params = LabelFrame(root, text="Параметры подключения")
lf_params.pack(fill="x", pady=5)

label_serv = tk.Label(lf_params, text=f'Сервер: {server_name}', font=("Arial", 10))
label_serv.pack(anchor=NW)

label_db = tk.Label(lf_params, text=f'База данных: {database_name}', font=("Arial", 10))
label_db.pack(anchor=NW)

lf_setup = LabelFrame(root, text="Настройка")
lf_setup.pack(fill="x")

label_select = tk.Label(lf_setup, text="Выбрать среду:", font=("Arial", 10))
label_select.pack(anchor=NW)

combobox_serv = ttk.Combobox(lf_setup, width=20, state="readonly", values=["dev", "test", "prod"])
combobox_serv.current(0)
combobox_serv.pack(anchor=NW)

label_select = tk.Label(lf_setup, text="Выбрать справочник:", font=("Arial", 10))
label_select.pack(anchor=NW)

combobox = ttk.Combobox(lf_setup, width=20, state="readonly",
                        values=["Центр затрат", "ТМЦ", "Стоимости ТМЦ", "Контрагенты"])
combobox.pack(anchor=NW)

download_btn = ttk.Button(lf_setup, text='Загрузить файл', width=20, command=lambda: upload_file())
download_btn.pack(anchor=NW, pady=5)
download_btn['state'] = 'disabled'


def selected(event):
    selection = get_table()
    label_log["text"] = f"Загрузить в таблицу: {selection}"
    if selection != -1:
        download_btn['state'] = 'normal'


combobox.bind("<<ComboboxSelected>>", selected)


def selected_serv(event):
    selection = get_server_ip()
    label_serv["text"] = f"Сервер: {selection}"


combobox_serv.bind("<<ComboboxSelected>>", selected_serv)

lf_log = LabelFrame(root, text="Инфолог")
lf_log.pack(fill="x", pady=5)

label_log = tk.Label(lf_log, text="Загрузчик готов к работе. Выберите справочник", font=("Arial", 10))
label_log.pack(anchor=NW)


def upload_file():
    f_types = [('Excel Files', '*.xlsx')]
    path = filedialog.askopenfilename(filetypes=f_types)
    if path:
        if check_template(path):
            upload_to_db(path)

# Check is correct template selected by user (compares field headers)
def check_template(file):
    df = pd.read_excel(file)
    inner_key_list = df.keys().tolist()
    if (combobox.current() == 0 and inner_key_list != in_costs_optima_field_list) or (
            combobox.current() == 1 and inner_key_list != in_Item_OptiMa_field_list) or (
            combobox.current() == 2 and inner_key_list != IN_ITEM_COSTS_OPTIMA_field_list) or (
            combobox.current() == 3 and inner_key_list != in_Supplier_OptiMa_field_list):
        label_log["text"] = 'Выбран некорректный шаблон'
        return False
    else:
        return True


def upload_to_db(file):
    df = pd.read_excel(file)
    server_name = get_server_ip()
    connection_string = f'''
        DRIVER={driver_name};
        SERVER={server_name};
        DATABASE={database_name};
        UID={user_name};
        PWD={user_password};        
        Trusted_Connection=no;
    '''

    connection_url = URL.create('mssql+pyodbc', query={
        'odbc_connect': connection_string})
    engine = create_engine(connection_url, module=odbc)

    try:
        engine.connect()
        print("Соединение установлено")
    except SQLAlchemyError as e:
        print(e)
        label_log["text"] = "Ошибка соединения"

    table_name = get_table()
    # fill tables
    if table_name == "In_Costs_Optima":
        upload_in_costs_optima(engine, df, table_name)
    elif table_name == "in_Item_OptiMa":
        upload_in_item_optima(engine, df, table_name)
    elif table_name == "IN_ITEM_COSTS_OPTIMA":
        upload_in_item_costs_optima(engine, df, table_name)
    elif table_name == "In_Supplier_Optima":
        upload_in_supplier_optima(engine, df, table_name)
    else:
        label_log["text"] = "Загрузка отменена.Таблица не определена"


# Before inserting, you need to rename the field headers from the excel template as they are named in the database
def upload_in_costs_optima(engine, df, table):
    df.rename(columns={'Наименование': 'DESCRIPTION',
                       'Номер центра затрат': 'MAINTENANCE_GLACCOUNT',
                       'Статус': 'ENABLED_FLAG',
                       'Дата активации': 'START_DATE_ACTIVE',
                       'Дата окончания срока действия': 'END_DATE_ACTIVE',
                       'Код организации': 'CompanyCODE'}, inplace=True)

    list_rec_id = []
    list_condition = []
    list_source_and_process_stamp = []
    list_dates = []
    iterator = 0

    # We need to make an iterator, but since the designers made a mistake and made the field a string,
    # we had to convert it and get the last value.
    # Although it was possible to simply set the identity for the field when designing the table.
    sql_query = '''SELECT COALESCE(MAX(CONVERT(Integer, INTERFACE_RECORD_ID)), '0') as id
                           FROM In_Costs_Optima 
                           order by MAX(CONVERT(Integer, INTERFACE_RECORD_ID)) desc'''

    INTERFACE_RECORD_ID_DF = pd.read_sql_query(sql_query, engine)

    if not INTERFACE_RECORD_ID_DF.empty:
        iterator = INTERFACE_RECORD_ID_DF.iloc[0]['id']

    count_records = 0
    for _ in df.iterrows():
        iterator += 1
        INTERFACE_RECORD_ID = str(iterator)
        list_rec_id.append(INTERFACE_RECORD_ID)
        list_condition.append("READY")
        list_source_and_process_stamp.append("SOA")
        list_dates.append(datetime.datetime.now())
        count_records += 1

    # I'm doing this conversion because this field is named differently on different servers.
    # This is a mistake of the designers, I had to get out of it.
    last_update_date_name = 'LastUpdateDate' if get_server_ip() == '172.25.241.22' else 'Last_Update_Date'

    df.insert(1, 'interface_record_id', list_rec_id, True)
    df.insert(2, 'interface_condition', list_condition, True)
    df.insert(3, 'source', list_source_and_process_stamp, True)
    df.insert(4, 'process_stamp', list_source_and_process_stamp, True)
    df.insert(5, 'date_time_stamp', list_dates, True)
    df.insert(6, f'{last_update_date_name}', list_dates, True)

    try:
        df.to_sql(table, engine, if_exists='append', index=False)
    except SQLAlchemyError as e:
        print(e)
        label_log["text"] = 'Ошибка при загрузке данных.'
    else:
        print('Данные успешно загружены')
        label_log["text"] = f"Данные успешно загружены. Создано строк: {count_records}."


# Before inserting, you need to rename the field headers from the excel template as they are named in the database
def upload_in_item_optima(engine, df, table):
    df.rename(columns={'Код': 'ITEM_CODE',
                       'Название на англ.яз.': 'DESCRIPTION',
                       'Категория': 'CATEGORY_SET_NAME',
                       'Товарная группа': 'CATEGORY_LEVEL4',
                       'Код товарной группы': 'ACCOUNTING_CATEGORY_ID',
                       'Название на рус.яз. ': 'RU_DESCRIPTION',
                       'Единица изм.(код)': 'PRIMARY_UOM_CODE',
                       'Единица изм.': 'PRIMARY_UNIT_OF_MEASURE',
                       'Кост центр': 'COST_CENTER',
                       'Закупочная категория ТМЦ, 1 сегмент': 'CATEGORY_LEVEL1',
                       'Закупочная категория ТМЦ, 2 сегмент': 'CATEGORY_LEVEL2',
                       'Закупочная категория ТМЦ, 3 сегмент': 'CATEGORY_LEVEL3',
                       'Идентификатор ТМЦ в ERP': 'INVENTORY_ITEM_ID',
                       'Закупочная категория ТМЦ, все сегменты': 'ITEM_CATEGORY_CONCAT',
                       'Код организации в ERP': 'OPERATING_UNIT',
                       'Наименование организации в ERP': 'OPERATING_UNIT_NAME',
                       'Код организации': 'CompanyCODE'}, inplace=True)

    list_condition = []
    list_process_stamp = []
    list_source = []
    list_dates = []
    list_item_status_code = []
    list_item_status = []
    list_item_type_category = []
    list_enabled_flag = []
    list_inv_category_concat = []
    list_item_type = []
    list_language_code = []
    list_organization_code = []
    list_organization_id = []
    list_status = []

    count_records = 0
    for _ in df.iterrows():
        list_condition.append("READY")
        list_source.append("NA")
        list_process_stamp.append("SOA")
        list_dates.append(datetime.datetime.now())
        list_item_status_code.append("Active")
        list_item_status.append("ACTIVE")
        list_item_type_category.append("ITEM")
        list_enabled_flag.append("Y")
        list_inv_category_concat.append("UNSPECIFIED.*")
        list_item_type.append("EAM")
        list_language_code.append("RU")
        list_organization_code.append("TSP")
        list_organization_id.append(261)
        list_status.append("X")
        count_records += 1

    df.insert(1, 'date_time_stamp', list_dates, True)
    df.insert(2, 'LastUpdateDate', list_dates, True)
    df.insert(3, 'interface_condition', list_condition, True)
    df.insert(4, 'source', list_source, True)
    df.insert(5, 'process_stamp', list_process_stamp, True)
    df.insert(6, 'item_status_code', list_item_status_code, True)
    df.insert(7, 'item_status', list_item_status, True)
    df.insert(8, 'item_type_category', list_item_type_category, True)
    df.insert(9, 'enabled_flag', list_enabled_flag, True)
    df.insert(10, 'ins_date', list_dates, True)
    df.insert(11, 'inv_category_concat', list_inv_category_concat, True)
    df.insert(12, 'item_type', list_item_type, True)
    df.insert(13, 'language_code', list_language_code, True)
    df.insert(14, 'organization_code', list_organization_code, True)
    df.insert(15, 'organization_id', list_organization_id, True)
    df.insert(16, 'status', list_status, True)
    # Same values as in source field
    df.insert(17, 'tariff_code', list_source, True)
    df.insert(18, 'shelf', list_source, True)

    try:
        df.to_sql(table, engine, if_exists='append', index=False)
    except SQLAlchemyError as e:
        print(e)
        label_log["text"] = 'Ошибка при загрузке данных.'
    else:
        print('Данные успешно загружены')
        label_log["text"] = f"Данные успешно загружены. Создано строк: {count_records}."

    return [df, count_records]


# Before inserting, you need to rename the field headers from the excel template as they are named in the database
def upload_in_item_costs_optima(engine, df, table):
    df.rename(columns={'Код организации': 'MAINTENANCE_ORGID',
                       'Код филиала': 'MAINTENANCE_SITEID',
                       'Код склада': 'LOCATION',
                       'Код ТМЦ': 'ITEM_CODE',
                       'Единица изм.(код)': 'UOM_CODE',
                       'Стоимость': 'UNIT_COST',
                       'Дата стоимости': 'PERIOD'}, inplace=True)

    list_rec_id = []
    list_source_and_process_stamp = []
    list_dates = []
    list_condition = []
    iterator = 0

    # We need to make an iterator, but since the designers made a mistake and made the field a string,
    # we had to convert it and get the last value.
    # Although it was possible to simply set the identity for the field when designing the table
    sql_query = '''SELECT COALESCE(MAX(CONVERT(Integer, INTERFACE_RECORD_ID)), '0') as id
                               FROM IN_ITEM_COSTS_OPTIMA 
                               order by MAX(CONVERT(Integer, INTERFACE_RECORD_ID)) desc'''

    INTERFACE_RECORD_ID_DF = pd.read_sql_query(sql_query, engine)

    if not INTERFACE_RECORD_ID_DF.empty:
        iterator = INTERFACE_RECORD_ID_DF.iloc[0]['id']

    count_records = 0
    for _ in df.iterrows():
        iterator += 1
        INTERFACE_RECORD_ID = str(iterator)
        list_rec_id.append(INTERFACE_RECORD_ID)
        list_source_and_process_stamp.append("SOA")
        list_dates.append(datetime.datetime.now())
        list_condition.append("READY")
        count_records += 1

    df.insert(1, 'interface_record_id', list_rec_id, True)
    df.insert(2, 'source', list_source_and_process_stamp, True)
    df.insert(3, 'process_stamp', list_source_and_process_stamp, True)
    df.insert(4, 'ins_date', list_dates, True)
    df.insert(5, 'creation_date', list_dates, True)
    df.insert(5, 'Last_Update_Date', list_dates, True)
    df.insert(6, 'interface_condition', list_condition, True)

    try:
        df.to_sql(table, engine, if_exists='append', index=False)
    except SQLAlchemyError as e:
        print(e)
        label_log["text"] = 'Ошибка при загрузке данных.'
    else:
        print('Данные успешно загружены')
        label_log["text"] = f"Данные успешно загружены. Создано строк: {count_records}."

# Before inserting, you need to rename the field headers from the excel template as they are named in the database
def upload_in_supplier_optima(engine, df, table):
    df.rename(columns={'Код': 'SUPPLIER_CODE',
                       'Имя/название': 'VENDOR_NAME',
                       'Основной адрес': 'ADDRESS',
                       'Страна': 'COUNTRY',
                       'ИНН': 'VAT_REGISTRATION_NUM',
                       'Описание': 'VENDOR_NAME_ALT',
                       'Код организации': 'CompanyCODE'}, inplace=True)

    list_rec_id = []
    list_condition = []
    list_process_stamp = []
    list_dates = []
    iterator = 0

    # We need to make an iterator, but since the designers made a mistake and made the field a string,
    # we had to convert it and get the last value.
    # Although it was possible to simply set the identity for the field when designing the table
    sql_query = '''SELECT COALESCE(MAX(CONVERT(Integer, INTERFACE_RECORD_ID)), '0') as id
                               FROM In_Supplier_Optima 
                               order by MAX(CONVERT(Integer, INTERFACE_RECORD_ID)) desc'''

    INTERFACE_RECORD_ID_DF = pd.read_sql_query(sql_query, engine)

    if not INTERFACE_RECORD_ID_DF.empty:
        iterator = INTERFACE_RECORD_ID_DF.iloc[0]['id']

    count_records = 0
    for _ in df.iterrows():
        iterator += 1
        INTERFACE_RECORD_ID = str(iterator)
        list_rec_id.append(INTERFACE_RECORD_ID)
        list_condition.append("READY")
        list_process_stamp.append("SOA")
        list_dates.append(datetime.datetime.now())
        count_records += 1

    df.insert(1, 'interface_record_id', list_rec_id, True)
    df.insert(2, 'interface_condition', list_condition, True)
    df.insert(3, 'process_stamp', list_process_stamp, True)
    df.insert(4, 'date_time_stamp', list_dates, True)
    df.insert(5, 'LastUpdateDate', list_dates, True)

    try:
        df.to_sql(table, engine, if_exists='append', index=False)
    except SQLAlchemyError as e:
        print(e)
        label_log["text"] = 'Ошибка при загрузке данных.'
    else:
        print('Данные успешно загружены')
        label_log["text"] = f"Данные успешно загружены. Создано строк: {count_records}."


def get_server_ip():
    selection = combobox_serv.current()
    if selection == 0:
        return '172.25.241.20'
    if selection == 1:
        return '172.25.241.22'
    if selection == 2:
        return '172.25.241.28'


def get_table():
    selection = combobox.current()
    if selection == 0:
        return 'In_Costs_Optima'
    if selection == 1:
        return 'in_Item_OptiMa'
    if selection == 2:
        return 'IN_ITEM_COSTS_OPTIMA'
    if selection == 3:
        return 'In_Supplier_Optima'


root.mainloop()
