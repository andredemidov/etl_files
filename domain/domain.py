import json
import pandas as pd


class LevelOne:

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

    def __str__(self):
        return self.name

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