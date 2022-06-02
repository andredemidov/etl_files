import json
from datetime import datetime
import os
from time import ctime
import re
import shutil
import requests
import pandas as pd


class Neosintez:

    TOKEN = None
    ROOTS = []

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
    def get_id_by_name(parent_id, class_id, name):
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
            raise None
        else:
            return Neosintez.create_in_neosintez(parent_id, class_id, name)

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
            # print(req_body)
            # print(response.text)
            pass
        return response

    @staticmethod
    def get_roots_from_neosintez():
        req_url = url + 'api/objects/search?take=100'
        payload = json.dumps({
            "Filters": [
                {
                    "Type": 5,
                    "Value": root_class_id  # id класса в Неосинтез
                }
            ],
            "Conditions": [  # условия для поиска в Неосинтез
                {
                    "Type": 1,  # тип условия 1 - атрибут
                    "Attribute": f'{config_attribute_id}',  # id атрибута в Неосинтез
                    "Operator": 7  # оператор сравнения. 7 - существует
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

            Root(id, keys_list, object_request_body)


class Root(Neosintez):

    def __init__(self, id, keys_list: list, object_request_body):
        self.root_id = id
        self.keys_list = keys_list
        self.object_request_body = object_request_body
        self.levels_one = self.__init_level_one()
        Neosintez.ROOTS.append(self)

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
        self.delete_items_id = None
        self.neosintez_items = None
        self.level_two = {}

    def __str__(self):
        return self.name

    def get_data_from_excel(self):
        f_list = [f for f in os.listdir(path=files_directory) if self.name in f and 'РД' in f]
        if f_list:
            f_date = [ctime(os.path.getctime(files_directory + f)) for f in f_list]
            self.f_path = files_directory + f_list[f_date.index(max(f_date))]
            self.data = pd.read_excel(self.f_path, sheet_name='TDSheet', converters={'№ поз. по ГП': str, 'Изм.': str})
            self.data.sort_values('Подобъект', inplace=True)
            self.data['Изм.'] = self.data['Изм.'].map(lambda x: '0' if x != x else x)

    def delete_file(self):
        os.remove(self.f_path)

    def get_data_from_neosintez(self):
        req_url = url + 'api/objects/search?take=20000'
        payload = json.dumps({
            "Filters": [
                {
                    "Type": 4,
                    "Value": self.parent  # id папки в Неосинтез
                },
                {
                    "Type": 5,
                    "Value": item_class_id  # id класса в Неосинтез
                }
            ]
        })
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
        import_item_key_set = set(self.data['Обозначение'].tolist())

        canseled_items_set = item_key_set - import_item_key_set
        canseled_items = tuple(filter(lambda x: x[1] in canseled_items_set, items.items()))

        canseled_items_id = set(item[0] for item in canseled_items)

        self.delete_items_id = canseled_items_id | double_items_id
        return self.delete_items_id

    def delete_items(self):
        counter = 0
        if self.delete_items_id:
            for item in self.delete_items_id:
                req_url = url + f'api/objects/{item}/parent?parentId={bin_item_id}'
                headers = {
                    'Accept': 'application/json',
                    'Authorization': f'Bearer {Root.TOKEN}',
                    'Content-Type': 'application/json-patch+json'
                }
                response = requests.put(req_url, headers=headers)
                if response.status_code == 200:
                    counter += 1
        return counter

    def get_level_two_names(self):
        level_two_names = tuple(pd.unique(self.data['Подобъект']))
        for level_two in level_two_names:
            # id = LevelOne.get_id_by_name(self.id, level_two_class_id, level_two)
            id = LevelOne.get_id_by_name(self.parent, level_two_class_id, level_two)
            self.level_two[level_two] = id

    def push_into_neosintez(self):
        for i, row in self.data.iterrows():
            item = Item(row['Обозначение'], self.level_two[row['Подобъект']], dict(row), self.object_request_body)
            item.push_into_neosintez()


class Item(Neosintez):

    ATTRIBUTES_MAPPING = None  # дата фрейм для мэпинга атрибутов и колонок эксель файла

    def __init__(self, key_value, parent_id, attributes_value, object_request_body):
        self.key = key_value
        self.parent_id = parent_id
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
        self.neosintez_id = None
        self.get_attributes_mapping(Item)

    def __str__(self):
        return self.key

    @staticmethod
    def get_attributes_mapping(cls):
        if cls.ATTRIBUTES_MAPPING is None:
            cls.ATTRIBUTES_MAPPING = pd.read_excel('default_attributes.xlsx', sheet_name='Лист1')

    @staticmethod
    def ref_atr(*, value, atr):
        value = value.replace('.', '')
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

    def get_request_body(self):
        func_dict = {
            2: self.str_atr,
            8: self.ref_atr,
        }
        if len(self.request_body) == 1:

            for j, attribute in self.ATTRIBUTES_MAPPING.iterrows():
                atr_value = str(self.attributes_value[attribute['name']])
                atr_id = attribute['id']
                atr_type = attribute['type']
                if atr_value == 'nan':  # пропустить если значение пустое
                    continue

                atr_value = func_dict.get(atr_type, self.str_atr)(value=atr_value, atr=attribute)

                if atr_value is None:  # пропустить если значение пустое
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
            self.neosintez_id = self.get_id_by_key(self.parent_id, item_class_id, self.key, self.key, key_attribute_id)

        self.get_request_body()
        self.put_attributes(self.neosintez_id, self.request_body)


def get_time():
    """Функция возвращает текущую дату и время в строке формата
    Y-m-d_H.M.S"""
    return f'{datetime.now().strftime("%Y-%m-%d_%H.%M.%S")}'

# создание файла для логов
file_name = f'log/{get_time()}.txt'
log = open(file_name, 'w')



with open('config.json', encoding='utf-8') as config:
    config_dict = json.loads(config.read())

url = config_dict['url']
root_class_id = config_dict['root_class_id']
config_attribute_id = config_dict['config_attribute_id']
files_directory = config_dict['files_directory']
level_one_class_id = config_dict['level_one_class_id']
level_two_class_id = config_dict['level_two_class_id']
item_class_id = config_dict['item_class_id']
key_attribute_id = config_dict['key_attribute_id']
object_attribute_id = config_dict['object_attribute_id']
bin_item_id = config_dict['bin_item_id']

Neosintez.get_token()
Neosintez.get_roots_from_neosintez()
print(Neosintez.ROOTS)
for root in Neosintez.ROOTS:
    print('Обработка', root.root_id, end=' ', file=log)
    for level_one in root.levels_one:
        print(level_one.name, file=log)

        level_one.get_data_from_excel()
        if level_one.data is None:
            print('Не найден файл', file=log)
            continue

        print('Количество комплектов в выгрузке', len(level_one.data), file=log)

        level_one.get_delete_items()

        print('Удалить комплектов', len(level_one.delete_items_id), file=log)

        level_one.delete_items()
        level_one.get_level_two_names()
        level_one.push_into_neosintez()

        print('Количество в Неосинтез в итоге:', len(level_one.neosintez_items), file=log)

        level_one.delete_file()

log.close()