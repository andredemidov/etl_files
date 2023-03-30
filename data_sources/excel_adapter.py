import os
import json
import re
from datetime import datetime
import pandas as pd


class ExcelAdapter:

    def __init__(self, mode, files_directory: str, suffix, mapping_data: dict, key_columns: list, key_column_name: str):
        self._mode = mode
        self._files_directory = files_directory
        self._mapping_data = mapping_data
        self._file_name = None
        self._suffix: str = suffix
        self._key_columns: list = key_columns
        self._key_column_name: str = key_column_name

    def get_data(self, key: str) -> list[dict]:
        """
        Method reads data from the file in files directory, serializes the data to list of dicts,
        performs extra handling if necessary and returns data
        :param key: uses to filter input data
        :return: list of serialized data
        """
        f_path = self._get_file_path(key)
        data = self._read_excel(f_path, key)
        data.drop_duplicates(inplace=True, subset=self._key_columns)
        data = data.to_json(orient='records', force_ascii=False)
        input_data = json.loads(data) if data else list()
        output_data = self._map_data(input_data)
        # create key column
        for item in output_data:
            values = [item[column] for column in self._key_columns if item[column]]
            item[self._key_column_name] = '-'.join(values)
        self._extra_handling(output_data)
        return output_data

    def _get_file_path(self, key):
        if self._mode == 'appius':
            f_list = [f for f in os.listdir(path=self._files_directory) if self._suffix in f and '~' not in f]
        else:
            f_list = [f for f in os.listdir(path=self._files_directory) if key in f and self._suffix in f and '~' not in f]
        if f_list:
            f_date = [os.path.getctime(self._files_directory + f) for f in f_list]
            self._file_name = f_list[f_date.index(max(f_date))]
            file_path = self._files_directory + self._file_name

        else:
            message = f'There is no file in {self._files_directory}'
            raise FileNotFoundError(message)

        return file_path

    def _read_excel(self, file_path, key):
        converters = {
            'appius': {
                'Объект генплана номер': str,
                'Номер изменения': str,
                'Проект системы.Код': str
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
                'Резервирование/Планирование закупок/Спецификация/РасходныйДокумент/Поступление/Уведомление об отгрузке.Номер': str,
            },
            'storage': {
                'Номенклатура.Код': str,
                'Объект резерва.Номер': str,
            }
        }
        data = pd.read_excel(
            file_path,
            sheet_name='TDSheet',
            converters=converters[self._mode]
        )
        if self._mode == 'appius':
            if not key:
                raise ValueError('There is no "key" parameter to filter input data')
            data = data[(data['Проект системы.Код'] == key)]
        return data

    def _extra_handling(self, input_data: list[dict]):
        if self._mode == 'appius':
            input_data = list(filter(lambda x: x['Обозначение'] and 'ЛСР' not in x['Обозначение'], input_data))
            for item in input_data:
                item['Номер изменения'] = item['Номер изменения'] if item['Номер изменения'] else '0'
                subobject = item['Объект строительства']
                item['Объект строительства'] = subobject if subobject else 'подобъект не указан'

        elif self._mode == 'notification':
            for item in input_data:
                date_delivery = item.get('Плановая дата прихода на склад')
                date_delivery = datetime.strptime(date_delivery, '%Y-%m-%d') if date_delivery else None
                item['Папка'] = "Приход " + date_delivery.strftime('%Y.%m') if date_delivery else 'нет'

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
                    atr_value = atr_value.rstrip('.')
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
        format_string = kwargs.get('format', "%Y-%m-%d")
        if isinstance(value, str):
            if len(value) > 10:
                value_date = datetime.strptime(value, '%d.%m.%Y %H:%M:%S')
            else:
                value_date = datetime.strptime(value, '%d.%m.%Y')
        else:
            value_date = value
        value = value_date.strftime(format_string)
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
        Method encapsulates the action upon finish. Now there is one action to move the handled file to the prev folder
        :return:
        """
        if self._mode != 'appius':
            f_path = self._files_directory + self._file_name
            f_prev_path = self._files_directory + 'prev/prev_' + self._file_name
            os.replace(f_path, f_prev_path)
