import os
import json
import re
from datetime import datetime
import pandas as pd


class ExcelAdapter:

    def __init__(self, files_directory: str, suffix, mapping_data: dict):
        self._files_directory = files_directory
        self._mapping_data = mapping_data
        self._file_name = None
        self._suffix: str = suffix

    def get_data(self, mode: str, name: str) -> list[dict]:
        """
        Method reads data from the file in files directory, serializes the data to list of dicts,
        performs extra handling if necessary and returns data
        :param mode: uses to detect file suffix.
        :param name: part of file name
        :return: list of serialized data
        """
        f_path = self._get_file_path(mode, name)
        data = self._read_excel(f_path, mode).to_json(orient='records', force_ascii=False)
        input_data = json.loads(data) if data else list()
        output_data = self._map_data(input_data)
        self._extra_handling(mode, output_data)
        return output_data

    def _get_file_path(self, mode, name):
        f_list = [f for f in os.listdir(path=self._files_directory) if name in f and self._suffix in f and '~' not in f]
        if f_list:
            f_date = [os.path.getctime(self._files_directory + f) for f in f_list]
            self._file_name = f_list[f_date.index(max(f_date))]
            _file_path = self._files_directory + self._file_name

        else:
            message = f'There is no file in {self._files_directory}'
            raise FileNotFoundError(message)

        return _file_path

    @staticmethod
    def _read_excel(file_path, mode):
        converters = {
            'appius': {
                '№ поз. по ГП': str,
                'Изм.': str
            },
            'mto': {
                'Код (НСИ)': str,
                'Потребность.Номер': str,
                'Потребность.Этап согласования': str
            },
            'delivery_order': {
                'Документ заказа.Номер': str,
                'Потребность.Номенклатура.Код': str,
                'Потребность.Номер': str,
            },
            'notification': {
                'Потребность.Номенклатура.Код': str,
                'Потребность.Номер': str,
                'Плановая дата прихода на склад': str,
                'Дата отгрузки': str,
            },
            'storage': {
                'Номенклатура.Код': str,
                'Объект резерва.Номер': str,
            }
        }
        data = pd.read_excel(
            file_path,
            sheet_name='TDSheet',
            converters=converters[mode]
        )
        return data

    @staticmethod
    def _extra_handling(mode, input_data: list[dict]):
        if mode == 'appius':
            input_data = list(filter(lambda x: x['Обозначение'] and 'ЛСР' not in x['Обозначение'], input_data))
            for item in input_data:
                item['Изм.'] = item['Изм.'] if item['Изм.'] else '0'
        elif mode == 'delivery_order':
            for item in input_data:
                item['Заказ-Потребность'] = item['Документ заказа.Номер'] + "-" + item['Потребность.Номер']
            input_data.sort(key=lambda x: x['Документ заказа.Номер'])
        elif mode == 'notification':
            for item in input_data:
                date_delivery = item.get('Плановая дата прихода на склад')  # yyyy-mm-dd
                date_delivery = datetime.strptime(date_delivery, '%Y-%m-%d') if date_delivery else None

                date_ship = item.get('Дата отгрузки')
                date_ship = datetime.strptime(date_ship, '%Y-%m-%d') if date_delivery else None

                delivery = date_delivery.strftime('%d.%m.%Y') if date_delivery else 'нет'
                ship = date_ship.strftime('%d.%m.%Y') if date_ship else 'нет'
                key = '-'.join([item['Потребность.Номер'], ship, delivery])
                item['Потребность-Дата отгрузки-Дата прихода'] = key

                item['Папка'] = "Приход " + date_delivery.strftime('%Y.%m') if date_delivery else 'нет' # date_delivery[:4] + '.' + date_delivery[5:7]

            input_data.sort(key=lambda x: x['Плановая дата прихода на склад'])
        elif mode == 'storage':
            for item in input_data:
                item['Потребность-Склад'] = item['Объект резерва.Номер'] + '-' + item['Склад']

    def _map_data(self, input_data) -> list[dict]:
        func_dict = {
            1: self._float_atr,
            2: self._str_atr,
            3: self._date_atr,
            5: self._date_atr,
            8: self._str_atr,
        }
        data = list()
        for item in input_data:
            item_dict = {}
            for attribute in self._mapping_data:
                name = attribute['name']
                atr_type = attribute['type']
                atr_value = item.get(name)
                if atr_type == 8 and isinstance(atr_value, str):
                    atr_value = atr_value.replace('.', '')
                if attribute['regexp']:
                    atr_value = self._get_by_re(atr_value, attribute['regexp'])
                    name = attribute['regexp_name']
                if atr_value:
                    atr_value = func_dict.get(atr_type, self._str_atr)(value=atr_value, atr=attribute)
                item_dict[name] = atr_value
            # level_two_name = item.get(level_two_column_name, "Прочее")
            # item_dict[level_two_column_name] = level_two_name if level_two_name else "Прочее"
            data.append(item_dict)
        return data

    @staticmethod
    def _str_atr(**kwargs):
        return kwargs['value'].strip() if kwargs['value'] else None

    @staticmethod
    def _float_atr(**kwargs):
        return float(kwargs['value'])

    @staticmethod
    def _date_atr(**kwargs):
        value = kwargs['value']
        format = kwargs.get('format', "%Y-%m-%d")
        if isinstance(value, str):
            if len(value) > 10:
                value_date = datetime.strptime(value, '%d.%m.%Y %H:%M:%S')
            else:
                value_date = datetime.strptime(value, '%d.%m.%Y')
        else:
            value_date = value
        value = value_date.strftime(format)
        return value if value_date.year > 2000 else None

    @staticmethod
    def _get_by_re(text, regexp):
        match = re.search(regexp, text)
        if match:
            result = match.group(1)
        else:
            result = None
        return result

    def finish(self):
        """
        Method incapsulates the action upon finish. Now there is one action to move the handled file to the prev folder
        :return:
        """
        f_path = self._files_directory + self._file_name
        f_prev_path = self._files_directory + 'prev/prev_' + self._file_name
        os.replace(f_path, f_prev_path)
