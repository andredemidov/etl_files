import sys
import json
from datetime import datetime
import os
from time import ctime
import re
from typing import List
import shutil
import requests
import pandas as pd


class Neosintez:

    TOKEN = None
    ROOTS:List[Root] = []

    @staticmethod
    def get_token():

        with open(config_dict['auth_data_file']) as f:
            aut_string = f.read()
        req_url = url + 'connect/token'
        payload = aut_string  # строка вида grant_type=password&username=????&password=??????&client_id=??????&client_secret=??????
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        response = requests.post(req_url, data=payload, headers=headers)
        if response.status_code == 200:
            Neosintez.TOKEN = json.loads(response.text)['access_token']
        return Neosintez.TOKEN

    @staticmethod
    def get_id_by_name(parent_id, class_id, name, create=False):
        req_url = url + 'api/objects/search?take=3'
        payload = json.dumps({
            "Filters": [
                {
                    "Type": 4,
                    "Value": parent_id  # id узла поиска в Неосинтез
                },
                {
                    "Type": 5,
                    "Value": class_id  # id класса в Неосинтез
                }
            ],
            "Conditions": [
                {
                'Value': name,
                'Operator': 1,
                'Type': 2,
            }
            ]
        })
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {Neosintez.TOKEN}',
            'Content-Type': 'application/json-patch+json',
            'X-HTTP-Method-Override': 'GET'
        }
        response = requests.post(req_url, headers=headers, data=payload)
        response = json.loads(response.text)
        if response['Total'] == 1:
            return response['Result'][0]['Object']['Id']
        elif response['Total'] > 1:
            print(f'Найдено более одно результата поиска Корень {parent_id}, класс {class_id}, имя {name}')
            return None
        elif create:
            return Neosintez.create_in_neosintez(parent_id, class_id, name)
        else:
            return ''

    @staticmethod
    def create_in_neosintez(parent_id, class_id, name):

        req_url = url + f'api/objects?parent={parent_id}'
        payload = json.dumps({
            "Id": "00000000-0000-0000-0000-000000000000",
            "Name": name,
            "Entity": {
                "Id": class_id,
                "Name": "forvalidation"
            }
        })
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {Neosintez.TOKEN}',
            'Content-Type': 'application/json-patch+json'
        }
        response = requests.post(req_url, headers=headers, data=payload)  # создание объекта
        response_text = json.loads(response.text)  # создание объекта с десериализацией ответа
        if response.status_code == 200:
            return response_text['Id']
        else:
            return ''

    @staticmethod
    def get_id_by_key(parent_id, class_id, name, value, attribute_value_id):
        req_url = url + 'api/objects/search?take=30'
        payload = json.dumps({
            "Filters": [
                {
                    "Type": 4,
                    "Value": parent_id  # id узла поиска в Неосинтез
                },
                {
                    "Type": 5,
                    "Value": class_id  # id класса в Неосинтез
                }
            ],
            "Conditions":  [
                {
                'Value': value,
                'Operator': 1,
                'Type': 1,
                'Attribute': attribute_value_id,
            }
            ]
        })
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {Neosintez.TOKEN}',
            'Content-Type': 'application/json-patch+json',
            'X-HTTP-Method-Override': 'GET'
        }
        response = requests.post(req_url, headers=headers, data=payload)
        response_text = json.loads(response.text)
        if response.status_code == 200 and response_text['Total'] == 1:
            return response_text['Result'][0]['Object']['Id']
        elif response.status_code == 200 and response_text['Total'] > 1:
            return None
        else:
            id = Neosintez.create_in_neosintez(parent_id, class_id, name)
            request_body = [{
                    'Name': 'forvalidation',
                    'Value': value,
                    'Type': 2,
                    'Id': attribute_value_id
                }]
            Neosintez.put_attributes(id, request_body)
            return id

    @staticmethod
    def put_attributes(item_id, request_body):
        req_url = url + f'api/objects/{item_id}/attributes'  # id сущности, в которой меняем атрибут
        payload = json.dumps(request_body)  # тело запроса в виде списка/словаря

        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {Neosintez.TOKEN}',
            'Content-Type': 'application/json-patch+json'
        }
        response = requests.put(req_url, headers=headers, data=payload)
        if response.status_code != 200:
            print(req_url)
            print(request_body)
            print(response.text)
            pass
        return response

    @staticmethod
    def get_roots_from_neosintez():
        req_url = url + 'api/objects/search?take=100'
        payload = json.dumps({
            "Filters": [
                {
                    "Type": 5,
                    "Value": root_class_id,
                }
            ],
            "Conditions": [
                {
                    "Type": 1,
                    "Attribute": f'{config_attribute_id}',
                    "Operator": 7
                }
            ]
        })
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {Neosintez.TOKEN}',
            'Content-Type': 'application/json-patch+json',
            'X-HTTP-Method-Override': 'GET'
        }
        response = json.loads(
            requests.post(req_url, headers=headers, data=payload).text)  # поисковый запрос с десериализацией ответа
        for folder in response['Result']:
            id = folder['Object']['Id']
            keys_list = folder['Object']['Attributes'][config_attribute_id]['Value'].split(';')
            object_request_body = folder['Object']['Attributes'].get(object_attribute_id, None)
            if object_request_body:
                object_request_body = object_request_body['Value']

            root = Root(id, keys_list, object_request_body)
            Neosintez.ROOTS.append(root)

    @staticmethod
    def get_by_re(text, regexp):
        match = re.search(regexp, text)
        if match:
            result = match.group(1)
        else:
            result = ''
        return result

    @staticmethod
    def ref_atr(*, value, atr):
        value = value.replace('.', '')
        if value == '80633-Р-2465/4200-ТК3':
            print()
        folder_id = atr['folder']
        class_id = atr['class']
        id = LevelOne.get_id_by_name(folder_id, class_id, value)
        if id:
            return {'Id': id, 'Name': 'forvalidation'}
        else:
            return None

    @staticmethod
    def str_atr(*, value, atr):
        return value

    @staticmethod
    def float_atr(*, value, atr):
        return float(value)

    @staticmethod
    def date_atr(*, value, atr):
        if len(value) > 10:
            value_date = datetime.strptime(value, '%d.%m.%Y %H:%M:%S')
            value = value_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        else:
            value_date = datetime.strptime(value, '%d.%m.%Y')
            value = value_date.strftime("%Y-%m-%d")
        return value if value_date.year > 2000 else None


class Root(Neosintez):

    def __init__(self, id, keys_list: list, object_request_body):
        self.root_id = id
        self.keys_list = keys_list
        self.object_request_body = object_request_body
        self.levels_one: List[LevelOne] = self.__init_level_one()


    def __init_level_one(self):
        return list(map(lambda name: LevelOne(name, self.root_id, self.object_request_body), self.keys_list))

    def __str__(self):
        return self.root_id

    def push_into_neosintez(self):
        for level_one in self.levels_one:
            level_one.get_data_from_excel()
            level_one.get_delete_items()
            level_one.delete_items()
            level_one.get_level_two_names()
            level_one.push_into_neosintez()


class LevelOne(Neosintez):

    def __init__(self, name, parent, object_request_body):
        self.name = name
        self.parent = parent
        self.object_request_body = object_request_body
        # self.id = self.get_id_by_name(parent, level_one_class_id, name)
        self.data = None
        self.update_data = None
        self.delete_items_id = None
        self.neosintez_items = None
        self.level_two = {}

    def __str__(self):
        return self.name

    @property
    def all_data_len(self):
        return len(self.data)

    @property
    def update_data_len(self):
        return len(self.update_data)

    def get_data_from_excel(self):
        prefix = {
            'appius': 'РД',
            'mto': 'ЗО',
            'delivery_order': 'Д',
        }
        f_list = [f for f in os.listdir(path=files_directory) if self.name in f and prefix[mode] in f and '~' not in f]
        if f_list:
            f_date = [ctime(os.path.getctime(files_directory + f)) for f in f_list]
            self.f_path = files_directory + f_list[f_date.index(max(f_date))]
            self.f_prev_path = files_directory + f'prev/{self.name}_{prefix[mode]}_prev.xlsx'
            if mode == 'appius':
                self.data = pd.read_excel(
                    self.f_path,
                    sheet_name='TDSheet',
                    converters={
                        '№ поз. по ГП': str,
                        'Изм.': str
                    }
                )
                self.data['Изм.'] = self.data['Изм.'].map(lambda x: '0' if x != x else x)

            elif mode == 'mto':
                self.data = pd.read_excel(
                    self.f_path,
                    sheet_name='TDSheet',
                    converters={
                        'Код (НСИ)': str,
                        'Потребность.Номер': str
                    }
                )
            elif mode == 'delivery_order':
                self.data = pd.read_excel(
                    self.f_path,
                    sheet_name='TDSheet',
                    converters={
                        'Документ заказа.Номер': str,
                        'Потребность.Номенклатура.Код': str,
                        'Потребность.Номер': str,
                    }
                )
                self.data['Заказ-Потребность'] = self.data['Документ заказа.Номер'] + "-" + self.data['Потребность.Номер']
                self.data.sort_values('Документ заказа.Номер', inplace=True)

    def get_update_data(self):
        if mode == 'appius':
            if os.path.isfile(self.f_prev_path):
                xl_prev = pd.read_excel(
                        self.f_path,
                        sheet_name='TDSheet',
                        converters={
                            '№ поз. по ГП': str,
                            'Изм.': str
                        }
                    )
                # формирование дата фрейма только вновь добавленных или изменных потребностей
                self.update_data = pd.concat([self.data, xl_prev]).drop_duplicates(keep=False)
                self.update_data.drop_duplicates(key_column_name, inplace=True)
            else:
                self.update_data = self.data.copy()
            self.update_data.sort_values('Подобъект', inplace=True)

        elif mode == 'mto':
            if os.path.isfile(self.f_prev_path):
                xl_prev = pd.read_excel(
                    self.f_prev_path,
                    sheet_name='TDSheet',
                    converters={
                        'Код (НСИ)': str,
                        'Потребность.Номер': str
                    }
                )
                # формирование дата фрейма только вновь добавленных или изменных потребностей
                self.update_data = pd.concat([self.data, xl_prev]).drop_duplicates(keep=False)
                self.update_data.drop_duplicates(key_column_name, inplace=True)
            else:
                self.update_data = self.data.copy()
            self.update_data.sort_values('Подобъект', inplace=True)
        elif mode == 'delivery_order':
            if os.path.isfile(self.f_prev_path):
                xl_prev = pd.read_excel(
                    self.f_prev_path,
                    sheet_name='TDSheet',
                    converters={
                        'Документ заказа.Номер': str,
                        'Потребность.Номенклатура.Код': str,
                        'Потребность.Номер': str,
                    }
                )
                xl_prev['Заказ-Потребность'] = xl_prev['Документ заказа.Номер'] + "-" + xl_prev[
                    'Потребность.Номер']
                # формирование дата фрейма только вновь добавленных или изменных записей
                self.update_data = pd.concat([self.data, xl_prev]).drop_duplicates(keep=False)
                self.update_data.drop_duplicates(key_column_name, inplace=True)
            else:
                self.update_data = self.data.copy()
            self.update_data.sort_values('Документ заказа.Номер', inplace=True)

    def copy_delete_file(self):
        shutil.copy2(self.f_path, self.f_prev_path)
        os.remove(self.f_path)

    def get_data_from_neosintez(self):
        req_url = url + 'api/objects/search?take=50000'
        payload = {
            "Filters": [
                {
                    "Type": 4,
                    "Value": self.parent
                },
                {
                    "Type": 5,
                    "Value": item_class_id
                }
            ]
        }
        if mode == 'mto':
            payload['Conditions'] = [
                {
                    "Value": self.name,
                    "Type": 1,
                    "Attribute": level_one_name_attribute_id,
                    "Operator": 1
                }
            ]
        payload = json.dumps(payload)
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {Neosintez.TOKEN}',
            'Content-Type': 'application/json-patch+json',
            'X-HTTP-Method-Override': 'GET'
        }
        # поисковый запрос
        response = requests.post(req_url, headers=headers, data=payload)
        response = json.loads(response.text)
        if response['Result']:
            # извлечение словаря, где ключ - id, значение - занчение ключевого атрибута
            self.neosintez_items = dict(map(lambda x: (
                x['Object']['Id'], x['Object']['Attributes'][key_attribute_id]['Value']),
                             response['Result']))
        else:
            self.neosintez_items = {}

    @property
    def total_in_neosintez(self):
        req_url = url + 'api/objects/search?take=0'
        payload = {
            "Filters": [
                {
                    "Type": 4,
                    "Value": self.parent
                },
                {
                    "Type": 5,
                    "Value": item_class_id
                }
            ]
        }
        if mode == 'mto':
            payload['Conditions'] = [
                {
                    "Value": self.name,
                    "Type": 1,
                    "Attribute": level_one_name_attribute_id,
                    "Operator": 1
                }
            ]
        payload = json.dumps(payload)
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {Neosintez.TOKEN}',
            'Content-Type': 'application/json-patch+json',
            'X-HTTP-Method-Override': 'GET'
        }
        # поисковый запрос
        response = requests.post(req_url, headers=headers, data=payload)
        response = json.loads(response.text)
        return response['Total']


    def get_delete_items(self):
        if self.neosintez_items is None:
            self.get_data_from_neosintez()
        items = self.neosintez_items
        # кортеж идентификаторов дублей по ключевому атрибуту
        double_items = tuple(filter(lambda k: k[1] > 1, map(lambda x: (x[0], list(items.values()).count(x[1])),
                                                               items.items())))
        double_items_id = set(item[0] for item in double_items)

        # множество номеров потребностей
        item_key_set = set(items.values())
        import_item_key_set = set(self.data[key_column_name].tolist())

        canseled_items_set = item_key_set - import_item_key_set
        canseled_items = tuple(filter(lambda x: x[1] in canseled_items_set, items.items()))

        canseled_items_id = set(item[0] for item in canseled_items)

        self.delete_items_id = canseled_items_id | double_items_id
        return self.delete_items_id

    def delete_items(self):
        counter = 0
        if self.delete_items_id:
            for item in self.delete_items_id:
                req_url = url + f'api/objects/{item}'
                headers = {
                    'Accept': 'application/json',
                    'Authorization': f'Bearer {Neosintez.TOKEN}',
                    'Content-Type': 'application/json-patch+json'
                }
                response = requests.delete(req_url, headers=headers)
                if response.status_code == 200:
                    counter += 1
        return counter

    def get_level_two_names(self):
        level_two_names = tuple(pd.unique(self.update_data[level_two_column_name]))
        for level_two in level_two_names:
            # id = LevelOne.get_id_by_name(self.id, level_two_class_id, level_two)
            id = LevelOne.get_id_by_name(self.parent, level_two_class_id, level_two, create=True)
            self.level_two[level_two] = id

    def push_into_neosintez(self):
        for i, row in self.update_data.iterrows():
            item = Item(
                row[key_column_name],
                self.level_two[row[level_two_column_name]],
                dict(row),
                self.object_request_body,
                self.name
            )
            item.push_into_neosintez()


class Item(Neosintez):

    # дата фрейм для мэпинга атрибутов и колонок эксель файла
    ATTRIBUTES_MAPPING = None

    def __init__(self, key_value, parent_id, attributes_value, object_request_body, level_one_name):
        self.key = key_value
        self.parent_id = parent_id
        self.level_one_name = level_one_name
        self.attributes_value = attributes_value
        self.object_request_body = object_request_body
        self.request_body = [
            {
                'Name': 'forvalidation',
                'Value': object_request_body,
                'Type': 8,
                'Id': object_attribute_id
            }
        ]
        if mode == 'mto':
            self.name = self.attributes_value['Номенклатурная позиция']
            self.request_body.append(
                {
                    'Name': 'forvalidation',
                    'Value': level_one_name,
                    'Type': 2,
                    'Id': level_one_name_attribute_id
                },
            )
        self.neosintez_id = None
        if mode == 'delivery_order':
            self.name = self.attributes_value['Потребность.Номенклатура.Наименование']


    def __str__(self):
        return self.key

    @staticmethod
    def get_attributes_mapping():
        if Item.ATTRIBUTES_MAPPING is None:
            Item.ATTRIBUTES_MAPPING = pd.read_excel(attributes_file, sheet_name='Лист1')


    def get_request_body(self):
        func_dict = {
            1: self.float_atr,
            2: self.str_atr,
            3: self.date_atr,
            5: self.date_atr,
            8: self.ref_atr,
        }
        for j, attribute in self.ATTRIBUTES_MAPPING.iterrows():
            atr_value = str(self.attributes_value[attribute['name']])
            atr_id = attribute['id']
            atr_type = attribute['type']
            if atr_value == 'nan':  # пропустить если значение пустое
                continue

            if str(attribute['regexp']) != 'nan':
                atr_value = self.get_by_re(atr_value, str(attribute['regexp']))

            atr_value = func_dict.get(atr_type, self.str_atr)(value=atr_value, atr=attribute)

            # пропустить если значение пустое
            if atr_value is None:
                continue

            atr_body = {
                'Name': 'forvalidation',
                'Value': atr_value,
                'Type': atr_type,
                'Id': atr_id
            }
            self.request_body.append(atr_body)

    def push_into_neosintez(self):
        if self.neosintez_id is None:
            name = self.name if mode != 'appius' else self.key
            self.neosintez_id = self.get_id_by_key(self.parent_id, item_class_id, name, self.key, key_attribute_id)

        self.get_request_body()
        self.put_attributes(self.neosintez_id, self.request_body)


def get_time():
    """Функция возвращает текущую дату и время в строке формата
    Y-m-d_H.M.S"""
    return f'{datetime.now().strftime("%Y-%m-%d_%H.%M.%S")}'

# создание файла для логов
file_name = f'log/{get_time()}.txt'
with open(file_name, 'w') as log:
    print(f'{get_time()}: Старт', file=log)



# mode может принимать значения appius или mto или delivery_order
mode = 'appius'

if len(sys.argv) == 2:
    mode = sys.argv[1]


with open(f'config_{mode}.json', encoding='utf-8') as config:
    config_dict = json.loads(config.read())

url = config_dict['url']
attributes_file = config_dict['attributes_file']
root_class_id = config_dict['root_class_id']
config_attribute_id = config_dict['config_attribute_id']
files_directory = config_dict['files_directory']
level_one_class_id = config_dict['level_one_class_id']
level_two_class_id = config_dict['level_two_class_id']
level_two_column_name = config_dict['level_two_column_name']
item_class_id = config_dict['item_class_id']
key_attribute_id = config_dict['key_attribute_id']
object_attribute_id = config_dict['object_attribute_id']
bin_item_id = config_dict['bin_item_id']
key_column_name = config_dict['key_column_name']
level_one_name_attribute_id = config_dict['level_one_name_attribute_id']

Neosintez.get_token()
Neosintez.get_roots_from_neosintez()
Item.get_attributes_mapping()
print(Neosintez.ROOTS)
for root in Neosintez.ROOTS:
    try:
        log = open(file_name, 'a')
        print(f'{get_time()}: Обработка', root.root_id, file=log)
        print(f'{get_time()}: Обработка', root.root_id)
        for level_one in root.levels_one:
            print(f'{get_time()}:', level_one.name, end='', file=log)
            print(level_one.name, end='')

            level_one.get_data_from_excel()

            if level_one.data is None:
                print(' Не найден файл', file=log)
                print(' Не найден файл')
                continue
            level_one.get_update_data()

            print('Количество строк в выгрузке', level_one.all_data_len, 'Обновить', level_one.update_data_len, file=log)
            print('Количество строк в выгрузке', level_one.all_data_len, 'Обновить', level_one.update_data_len)

            level_one.get_delete_items()

            print(f'{get_time()}: Удалить строк', len(level_one.delete_items_id), file=log)
            print(f'{get_time()}: Удалить строк', len(level_one.delete_items_id))

            level_one.delete_items()

            print(f'{get_time()}: Удаление завершено. Начато обновление', file=log)
            print(f'{get_time()}: Удаление завершено. Начато обновление')

            level_one.get_level_two_names()
            level_one.push_into_neosintez()

            print(f'{get_time()}: Количество в Неосинтез в итоге:', level_one.total_in_neosintez, file=log)
            print(f'{get_time()}: Количество в Неосинтез в итоге:', level_one.total_in_neosintez)

            level_one.copy_delete_file()

        log.close()
    except Exception as e:
        print()
        print(f'{get_time()}: {e}', file=log)
        print(f'{get_time()}: {e}')
        log.close()
        continue


