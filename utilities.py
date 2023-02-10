import json
import pandas as pd


class Utilities:

    CONFIG = None
    MAPPING_DATA = None

    @classmethod
    def read_config(cls, config_file_name_suffix: str) -> dict:
        if cls.CONFIG:
            return cls.CONFIG
        with open(f'configs/config_{config_file_name_suffix}.json', encoding='utf-8') as config:
            cls.CONFIG = json.loads(config.read())

        return cls.CONFIG

    @classmethod
    def mapping_data(cls):
        if cls.MAPPING_DATA:
            return cls.MAPPING_DATA
        mapping = pd.read_excel(cls.CONFIG['attributes_file'], sheet_name='Лист1')
        mapping = mapping.to_json(orient='records', force_ascii=False)
        mapping = json.loads(mapping)
        cls.MAPPING_DATA = mapping
        return cls.MAPPING_DATA
