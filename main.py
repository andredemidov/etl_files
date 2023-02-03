import sys
import json
import os
import re
import shutil
import logging
from datetime import datetime
from typing import List
import requests
import pandas as pd


class Neosintez:
    TOKEN = None
    ROOTS = []
    SESSION = None

    @staticmethod
    def get_token():

        with open(config_dict['auth_data_file']) as f:
            aut_string = f.read()
        req_url = url + 'connect/token'
        # строка вида grant_type=password&username=????&password=??????&client_id=??????&client_secret=??????
        payload = aut_string
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        Neosintez.SESSION = requests.session()
        response = Neosintez.SESSION.post(req_url, data=payload, headers=headers)
        if response.status_code == 200:
            Neosintez.TOKEN = json.loads(response.text)['access_token']

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
        response = Neosintez.SESSION.post(req_url, headers=headers, data=payload)
        response = json.loads(response.text)
        if response['Total'] == 1:
            return response['Result'][0]['Object']['Id']
        elif response['Total'] > 1:
            logging.warning(f'More then one result is found for {parent_id}, class id {class_id}, name {name}')
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
        response = Neosintez.SESSION.post(req_url, headers=headers, data=payload)  # создание объекта
        response_text = json.loads(response.text)  # создание объекта с десериализацией ответа
        if response.status_code == 200:
            return response_text['Id']
        else:
            logging.warning(f'Item is not created {name} {response.status_code} {response.text}')
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
            "Conditions": [
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
        response = Neosintez.SESSION.post(req_url, headers=headers, data=payload)
        response_text = json.loads(response.text)
        if response.status_code == 200 and response_text['Total'] == 1:
            return response_text['Result'][0]['Object']['Id']
        elif response.status_code == 200 and response_text['Total'] > 1:
            return None
        else:
            item_id = Neosintez.create_in_neosintez(parent_id, class_id, name)
            request_body = [{
                'Name': 'forvalidation',
                'Value': value,
                'Type': 2,
                'Id': attribute_value_id
            }]
            Neosintez.put_attributes(item_id, request_body)
            return item_id

    @staticmethod
    def put_attributes(item_id, request_body):
        req_url = url + f'api/objects/{item_id}/attributes'  # id сущности, в которой меняем атрибут
        payload = json.dumps(request_body)  # тело запроса в виде списка/словаря

        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {Neosintez.TOKEN}',
            'Content-Type': 'application/json-patch+json'
        }
        response = Neosintez.SESSION.put(req_url, headers=headers, data=payload)
        if response.status_code != 200:
            logging.warning(f'Put attributes error. Url {req_url}, body {request_body}, response {response.text}')
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
        response = json.loads(Neosintez.SESSION.post(req_url, headers=headers, data=payload).text)
        for folder in response['Result']:
            item_id = folder['Object']['Id']
            keys_list = folder['Object']['Attributes'][config_attribute_id]['Value']
            keys_list = keys_list.replace(' ', '')
            keys_list = keys_list.split(';')
            object_request_body = folder['Object']['Attributes'].get(object_attribute_id)
            if object_request_body:
                object_request_body = object_request_body['Value']

            next_root = Root(item_id, keys_list, object_request_body)
            Neosintez.ROOTS.append(next_root)

    @staticmethod
    def get_by_re(text, regexp):
        match = re.search(regexp, text)
        if match:
            result = match.group(1)
        else:
            result = None
        return result

    @staticmethod
    def ref_atr(**kwargs):
        value = kwargs['value']
        atr = kwargs['atr']
        value = value.replace('.', '')
        folder_id = atr['folder']
        class_id = atr['class']
        item_id = LevelOne.get_id_by_name(folder_id, class_id, value)
        if item_id:
            return {'Id': item_id, 'Name': 'forvalidation'}
        else:
            return None

    @staticmethod
    def str_atr(**kwargs):
        return kwargs['value']

    @staticmethod
    def float_atr(**kwargs):
        return float(kwargs['value'])

    @staticmethod
    def date_atr(**kwargs):
        value = kwargs['value']
        if len(value) > 10:
            value_date = datetime.strptime(value, '%d.%m.%Y %H:%M:%S')
            # value = value_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            value = value_date.strftime("%Y-%m-%d")
        else:
            value_date = datetime.strptime(value, '%d.%m.%Y')
            value = value_date.strftime("%Y-%m-%d")
        return value if value_date.year > 2000 else None


class Root(Neosintez):

    def __init__(self, item_id, keys_list: list, object_request_body):
        self.root_id = item_id
        self.keys_list = keys_list
        self.object_request_body = object_request_body
        self.levels_one: List[LevelOne] = self.__init_level_one()

    def __init_level_one(self):
        return list(map(lambda name: LevelOne(name, self.root_id, self.object_request_body), self.keys_list))

    def __str__(self):
        return self.root_id
    #
    # def push_into_neosintez(self):
    #     for next_level_one in self.levels_one:
    #         next_level_one._get_data_from_excel()
    #         next_level_one.get_delete_items()
    #         next_level_one.delete_items()
    #         next_level_one.get_level_two_names()
    #         next_level_one.push_into_neosintez()


class LevelOne(Neosintez):

    def __init__(self, name, parent, object_request_body):
        self.name = name
        self.parent = parent
        self.object_request_body = object_request_body
        # self.id = self.get_id_by_name(parent, level_one_class_id, name)
        self.new_data = None
        self.current_data = None
        self.update_data = None
        self.delete_items_id = None
        self.levels_two = {}
        self.f_path = ''
        self.f_prev_path = ''
        self._mapping_data = None

    @property
    def mapping_data(self):
        if not self._mapping_data:
            mapping = pd.read_excel(attributes_file, sheet_name='Лист1').to_json(orient='records', force_ascii=False)
            mapping = json.loads(mapping)
            self._mapping_data = mapping
        return self._mapping_data

    def __str__(self):
        return self.name

    def get_file_path(self):
        prefix = {
            'appius': 'РД',
            'mto': 'ЗО',
            'delivery_order': 'Д',
            'notification': 'У',
        }

        f_list = [f for f in os.listdir(path=files_directory) if self.name in f and prefix[mode] in f and '~' not in f]
        if f_list:
            f_date = [os.path.getctime(files_directory + f) for f in f_list]
            self.f_path = files_directory + f_list[f_date.index(max(f_date))]
            self.f_prev_path = files_directory + f'prev/{self.name}_{prefix[mode]}_prev.xlsx'

    def _get_data_from_excel(self) -> list[dict]:
        data = self._read_excel(self.f_path).to_json(orient='records', force_ascii=False)
        input_data = json.loads(data) if data else list()
        return input_data

    @staticmethod
    def _read_excel(file_path):
        if mode == 'appius':
            data = pd.read_excel(
                file_path,
                sheet_name='TDSheet',
                converters={
                    '№ поз. по ГП': str,
                    'Изм.': str
                }
            )
            data['Изм.'] = data['Изм.'].map(lambda x: '0' if x != x else x)
            data.dropna(subset='Обозначение', inplace=True)
            data = data[-data['Обозначение'].str.contains('ЛСР')]

        elif mode == 'mto':
            data = pd.read_excel(
                file_path,
                sheet_name='TDSheet',
                converters={
                    'Код (НСИ)': str,
                    'Потребность.Номер': str,
                    'Потребность.Этап согласования': str
                }
            )
        elif mode == 'delivery_order':
            data = pd.read_excel(
                file_path,
                sheet_name='TDSheet',
                converters={
                    'Документ заказа.Номер': str,
                    'Потребность.Номенклатура.Код': str,
                    'Потребность.Номер': str,
                }
            )
            data['Заказ-Потребность'] = data['Документ заказа.Номер'] + "-" + data['Потребность.Номер']
            data.sort_values('Документ заказа.Номер', inplace=True)
        elif mode == 'notification':
            data = pd.read_excel(
                file_path,
                sheet_name='TDSheet',
                converters={
                    'Потребность.Номенклатура.Код': str,
                    'Потребность.Номер': str,
                    'Плановая дата прихода на склад': str,
                    'Дата отгрузки': str,
                }
            )
            data['Потребность-Дата отгрузки-Дата прихода'] = data['Потребность.Номер'] + "-" + list(
                map(lambda x: x if isinstance(x, str) else 'нет', data['Дата отгрузки'])) + "-" + list(
                map(lambda x: x if isinstance(x, str) else 'нет', data['Плановая дата прихода на склад']))

            data['Папка'] = list(map(lambda x: LevelOne._get_level_two_name_for_notification(x),
                                     data['Плановая дата прихода на склад']))
            data.sort_values('Плановая дата прихода на склад', inplace=True)
        else:
            data = None
        return data

    # def __get_update_data(self):
    #     if os.path.isfile(self.f_prev_path):
    #         xl_prev = self._read_excel(self.f_prev_path)
    #         self.update_data = pd.concat([self.new_data, xl_prev]).drop_duplicates(keep=False)
    #         self.update_data.drop_duplicates(key_column_name, inplace=True)
    #     else:
    #         self.update_data = self.new_data.copy()
    #
    def delete_file(self):
        shutil.copy2(self.f_path, self.f_prev_path)
        os.remove(self.f_path)

    def _get_data_from_neosintez(self):
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
        response = Neosintez.SESSION.post(req_url, headers=headers, data=payload)
        response = json.loads(response.text)
        return response

    def get_current_items_data(self):
        response = self._get_data_from_neosintez()
        data = list()
        for item in response['Result']:
            item_dict = {'id': item['Object']['Id']}
            for attribute in self.mapping_data:
                atr_id = attribute['id']
                name = attribute['name']
                if attribute['regexp']:
                    name = attribute['regexp_name']
                result = item['Object']['Attributes'].get(atr_id)
                if result:
                    atr_value = result['Value']
                    if result['Type'] == 8:
                        atr_value = atr_value['Name']
                    elif result['Type'] == 3 or result['Type'] == 5:
                        atr_value = datetime.strptime(atr_value, '%Y-%m-%dT%H:%M:%S')
                        atr_value = atr_value.strftime("%Y-%m-%d")

                else:
                    atr_value = None

                item_dict[name] = atr_value

            data.append(item_dict)
        self.current_data = data

    def get_new_items_data(self):
        func_dict = {
            1: self.float_atr,
            2: self.str_atr,
            3: self.date_atr,
            5: self.date_atr,
            8: self.str_atr,
        }
        input_data = self._get_data_from_excel()
        data = list()
        for item in input_data:
            item_dict = {}
            for attribute in self.mapping_data:
                name = attribute['name']
                atr_type = attribute['type']
                atr_value = item.get(name)
                if atr_type == 8 and isinstance(atr_value, str):
                    atr_value = atr_value.replace('.', '')
                if attribute['regexp']:
                    atr_value = self.get_by_re(atr_value, attribute['regexp'])
                    name = attribute['regexp_name']
                if atr_value:
                    atr_value = func_dict.get(atr_type, self.str_atr)(value=atr_value, atr=attribute)
                item_dict[name] = atr_value
            level_two_name = item.get(level_two_column_name, "Прочее")
            item_dict[level_two_column_name] = level_two_name if level_two_name else "Прочее"
            data.append(item_dict)
        self.new_data = data

    def get_update_data(self):
        for_update = list()
        current_dict = dict(map(lambda x: (x[key_column_name], x), self.current_data))
        for new_item in self.new_data:
            match_item = current_dict.get(new_item[key_column_name])
            if match_item:
                match_item_for_compare = match_item.copy()
                del match_item_for_compare['id']
                new_item_for_compare = new_item.copy()
                if 'Папка' in new_item_for_compare:
                    del new_item_for_compare['Папка']
                if new_item_for_compare == match_item_for_compare:
                    continue
            for_update.append(new_item)
        self.update_data = for_update

    # def __get_keys_from_neosintez(self):
    #     response = self._get_data_from_neosintez()
    #     if response['Result']:
    #         # извлечение словаря, где ключ - id, значение - значение ключевого атрибута
    #         self.neosintez_items_keys = dict(map(lambda x: (
    #             x['Object']['Id'], x['Object']['Attributes'][key_attribute_id]['Value']),
    #                                              response['Result']))
    #     else:
    #         self.neosintez_items_keys = {}

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
        response = Neosintez.SESSION.post(req_url, headers=headers, data=payload)
        response = json.loads(response.text)
        return response['Total']

    def get_delete_items(self):

        items = dict(map(lambda x: (x['id'], x[key_column_name]), self.current_data))
        # кортеж идентификаторов дублей по ключевому атрибуту
        double_items = tuple(
            filter(lambda k: k[1] > 1, map(lambda x: (x[0], list(items.values()).count(x[1])), items.items())))
        double_items_id = set(item[0] for item in double_items)

        item_key_set = set(items.values())
        import_item_key_set = set(map(lambda x: x[key_column_name], self.new_data))

        canceled_items_set = item_key_set - import_item_key_set
        canceled_items = tuple(filter(lambda x: x[1] in canceled_items_set, items.items()))

        canceled_items_id = set(item[0] for item in canceled_items)

        self.delete_items_id = canceled_items_id | double_items_id

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
                response = Neosintez.SESSION.delete(req_url, headers=headers)
                if response.status_code == 200:
                    counter += 1
        return counter

    @staticmethod
    def _get_level_two_name_for_notification(s):
        if isinstance(s, str):
            return "Приход " + s[-4:] + '.' + s[-7:-5]
        else:
            return 'Дата прихода не указана'

    def get_level_two_names(self):
        level_two_names = set(map(lambda x: x[level_two_column_name], self.update_data))
        for level_two in level_two_names:
            # id = LevelOne.get_id_by_name(self.id, level_two_class_id, level_two)
            item_id = self.get_id_by_name(self.parent, level_two_class_id, level_two, create=True)
            self.levels_two[level_two] = item_id

    def push_into_neosintez(self):
        for item_data in self.update_data:
            item = Item(
                key_value=item_data[key_column_name],
                parent_id=self.levels_two[item_data[level_two_column_name]],
                attributes_value=item_data,
                object_request_body=self.object_request_body,
                level_one_name=self.name,
                mapping_data=self.mapping_data
            )
            item.push_into_neosintez()


class Item(Neosintez):
    # дата фрейм для мэпинга атрибутов и колонок эксель файла
    ATTRIBUTES_MAPPING = None

    def __init__(self, key_value, parent_id, attributes_value, object_request_body, level_one_name, mapping_data):
        self.key = key_value
        self.parent_id = parent_id
        self.level_one_name = level_one_name
        self.attributes_value = attributes_value
        self.object_request_body = object_request_body
        self.mapping_data = mapping_data
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
        if mode == 'delivery_order' or mode == 'notification':
            self.name = self.attributes_value['Потребность.Номенклатура.Наименование']

    def __str__(self):
        return self.key

    def get_request_body(self):
        func_dict = {
            1: self.float_atr,
            2: self.str_atr,
            3: self.str_atr,
            5: self.str_atr,
            8: self.ref_atr,
        }
        for attribute in self.mapping_data:
            atr_value = self.attributes_value.get(attribute['name'])
            atr_id = attribute['id']
            atr_type = attribute['type']
            if atr_value:
                if attribute['regexp']:
                    atr_value = self.get_by_re(atr_value, str(attribute['regexp']))

                atr_value = func_dict.get(atr_type, self.str_atr)(value=atr_value, atr=attribute)

            # # пропустить если значение пустое
            # if not atr_value:
            #     continue

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
    """Функция возвращает текущую дату и время в строке формата Y-m-d_H.M.S"""
    return f'{datetime.now().strftime("%Y-%m-%d")}'


# mode может принимать значения appius или mto или delivery_order или notification
DEBUG = False

if DEBUG:
    mode = 'appius'
    config_file_name_suffix = mode
elif len(sys.argv) != 3 and not DEBUG:
    raise EnvironmentError('При запуске должны быть переданы два аргумента: mode и config suffix')
else:
    mode = sys.argv[1]
    config_file_name_suffix = sys.argv[2]

with open(f'config_{config_file_name_suffix}.json', encoding='utf-8') as config:
    config_dict = json.loads(config.read())

url = config_dict['url']
logs_path = config_dict['logs_path']
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

logging.basicConfig(
    format='%(asctime)s : %(levelname)s : %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler(logs_path + get_time() + f'_{config_file_name_suffix}.log'),
        logging.StreamHandler()
    ]
)
try:
    Neosintez.get_token()
    Neosintez.get_roots_from_neosintez()
    logging.info(f'Total main roots {len(Neosintez.ROOTS)}')
    for root in Neosintez.ROOTS:
        try:
            logging.info(f'Processing main root {root.root_id}')
            for level_one in root.levels_one:
                logging.info(f'Processing level one {level_one.name}')

                level_one.get_file_path()
                if not level_one.f_path:
                    logging.warning('File is not found')
                    continue

                level_one.get_new_items_data()

                if not level_one.new_data:
                    logging.warning('File is empty')
                    level_one.delete_file()
                    logging.info('File is copied in prev folder')
                    continue

                logging.info(f'Total rows in new input file {len(level_one.new_data)}')

                level_one.get_current_items_data()
                logging.info(f'Total entities in neosintez at beginning {len(level_one.current_data)}')

                level_one.get_update_data()
                logging.info(f'Total entities for update {len(level_one.update_data)}')

                level_one.get_delete_items()
                logging.info(f'Total rows for delete {len(level_one.delete_items_id)}')
                logging.info('Deleting')
                deleted_counter = level_one.delete_items()
                logging.info(f'Deleting complete. Deleted {deleted_counter}')

                logging.info('Updating')
                level_one.get_level_two_names()
                level_one.push_into_neosintez()
                logging.info(f'Updating complete. Total in neosintez {level_one.total_in_neosintez}')

                level_one.delete_file()
                logging.info('File is copied in prev folder')

        except Exception as e:

            print(e)
            logging.exception('Error occurred')
finally:
    Neosintez.SESSION.close()
    logging.info('Session is closed')
