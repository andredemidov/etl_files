import json
import requests
import logging
import os.path
from datetime import datetime
from domain.entities import Construction, Item


class Neosintez:

    COMMON_ATTRIBUTES_ID = {
        'construction_class_id': '3aa54908-2283-ec11-911c-005056b6948b',
        'title_class_id': 'fa758bdc-0683-ec11-911c-005056b6948b',
        'titles_attribute_id': '4f3b1845-4d7c-ed11-9153-005056b6948b',
        'subobject_list_class_id': 'e2cba4ee-487c-ed11-9153-005056b6948b',
        'subobject_list_parent_id': 'b535c92d-4d7c-ed11-9153-005056b6948b',
        'delete_mark_attribute_id': '7d39b7fb-b9eb-ec11-9131-005056b6948b',
        'delete_mark_value_id': 'd5fa86ec-b9eb-ec11-9131-005056b6948b',
    }

    def __init__(self, url, config, mapping_data):
        self.url = url
        self._session_object = None
        self._token_keeper = None
        self._ref_atr_values = {}
        self._config = config
        self._mapping_data = mapping_data

    @property
    def _session(self):
        if self._session_object:
            return self._session_object
        self._session_object = requests.session()
        return self._session_object

    @property
    def _token(self):
        if self._token_keeper:
            return self._token_keeper
        else:
            if os.path.isfile('test_data/token.txt'):
                # read saved token for debug mode
                with open('test_data/token.txt') as token:
                    self._token_keeper = token.read()
            else:
                with open(self._config['auth_data_file']) as f:
                    # строка вида grant_type=password&username=??&password=????&client_id=????&client_secret=????
                    payload = f.read()
                req_url = self.url + 'connect/token'

                headers = {
                    'Content-Type': 'application/x-www-form-urlencoded'
                }
                response = self._session.post(req_url, data=payload, headers=headers)
                if response.status_code == 200:
                    self._token_keeper = json.loads(response.text)['access_token']
        return self._token_keeper

    def _get_id_by_name(self, parent_id, class_id, name, create=False):
        req_url = self.url + 'api/objects/search?take=3'
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
            'Authorization': f'Bearer {self._token}',
            'Content-Type': 'application/json-patch+json',
            'X-HTTP-Method-Override': 'GET'
        }
        response = self._session.post(req_url, headers=headers, data=payload)
        response = json.loads(response.text)
        if response['Total'] == 1:
            return response['Result'][0]['Object']['Id']
        elif response['Total'] > 1:
            logging.warning(f'More then one result is found for {parent_id}, class id {class_id}, name {name}')
            return None
        elif create:
            return self._create_in_neosintez(parent_id, class_id, name)
        else:
            return ''

    def _get_item_by_name(self, parent_id, class_id, name) -> list:
        req_url = self.url + 'api/objects/search?take=3'
        payload = json.dumps({
            "Filters": [
                {
                    "Type": 4,
                    "Value": parent_id
                },
                {
                    "Type": 5,
                    "Value": class_id
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
            'Authorization': f'Bearer {self._token}',
            'Content-Type': 'application/json-patch+json',
            'X-HTTP-Method-Override': 'GET'
        }
        response = self._session.post(req_url, headers=headers, data=payload)
        response = json.loads(response.text)
        if response['Total'] == 1:
            return response['Result']
        elif response['Total'] > 1:
            message = f'More then one result is found for {parent_id}, class id {class_id}, name {name}'
            raise LookupError(message)

    def _get_items_by_class(self, parent_id, class_id, take: int = 30000) -> dict:
        req_url = self.url + f'api/objects/search?take={take}'
        payload = json.dumps({
            "Filters": [
                {
                    "Type": 4,
                    "Value": parent_id
                },
                {
                    "Type": 5,
                    "Value": class_id
                }
            ]
        })
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {self._token}',
            'Content-Type': 'application/json-patch+json',
            'X-HTTP-Method-Override': 'GET'
        }
        response = self._session.post(req_url, headers=headers, data=payload)
        response = json.loads(response.text)
        return response

    def _create_in_neosintez(self, parent_id, class_id, name) -> str:
        """
        Method creates an entity in neosintez and return id. If error occurs it returns an empty string
        :param parent_id: parent id in neosintez
        :param class_id: class id in neosintez
        :param name: name of created entity
        :return: created entity id
        """

        req_url = self.url + f'api/objects?parent={parent_id}'
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
            'Authorization': f'Bearer {self._token}',
            'Content-Type': 'application/json-patch+json'
        }
        response = self._session.post(req_url, headers=headers, data=payload)
        response_text = json.loads(response.text)
        if response.status_code == 200:
            return response_text['Id']
        else:
            logging.warning(f'Item is not created {name} {response.status_code} {response.text}')
            return ''

    def _get_id_by_key(self, parent_id, class_id, name, value, attribute_value_id, create=False):
        req_url = self.url + 'api/objects/search?take=30'
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
            'Authorization': f'Bearer {self._token}',
            'Content-Type': 'application/json-patch+json',
            'X-HTTP-Method-Override': 'GET'
        }
        response = self._session.post(req_url, headers=headers, data=payload)
        response_text = json.loads(response.text)
        if response.status_code == 200 and response_text['Total'] == 1:
            return response_text['Result'][0]['Object']['Id']
        elif response.status_code == 200 and response_text['Total'] > 1:
            logging.warning(f'More then one result is found for {parent_id}, class id {class_id}, value {value}')
            return None
        elif create:
            item_id = self._create_in_neosintez(parent_id, class_id, name)
            request_body = [{
                'Name': 'forvalidation',
                'Value': value,
                'Type': 2,
                'Id': attribute_value_id
            }]
            self._put_attributes(item_id, request_body)
            return item_id
        else:
            return None

    def _put_attributes(self, item_id, request_body):
        req_url = self.url + f'api/objects/{item_id}/attributes'  # id сущности, в которой меняем атрибут
        payload = json.dumps(request_body)  # тело запроса в виде списка/словаря

        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {self._token}',
            'Content-Type': 'application/json-patch+json'
        }
        response = self._session.put(req_url, headers=headers, data=payload)
        if response.status_code != 200:
            logging.warning(f'Put attributes error. Url {req_url}, body {request_body}, response {response.text}')
        return response

    def get_constructions(self):
        req_url = self.url + 'api/objects/search?take=100'
        payload = json.dumps({
            "Filters": [
                {
                    "Type": 5,
                    "Value": self.COMMON_ATTRIBUTES_ID['construction_class_id'],
                }
            ],
            "Conditions": [
                {
                    "Type": 1,
                    "Attribute": self._config['config_attribute_id'],
                    "Operator": 7
                }
            ]
        })
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {self._token}',
            'Content-Type': 'application/json-patch+json',
            'X-HTTP-Method-Override': 'GET'
        }
        response = json.loads(self._session.post(req_url, headers=headers, data=payload).text)
        result = []
        for folder in response['Result']:
            self_id = folder['Object']['Id']
            name = folder['Object']['Name']
            key = folder['Object']['Attributes'][self._config['config_attribute_id']]['Value']
            construction = Construction(self_id=self_id, name=name, key=key)
            result.append(construction)
        return result

    def _get_request_body(self, item: Item) -> list:
        # remove delete mark
        request_body = [
            {
                'Name': 'forvalidation',
                'Value': None,
                'Type': 8,
                'Id': self.COMMON_ATTRIBUTES_ID['delete_mark_attribute_id']
            }
        ]
        for attribute in self._mapping_data:
            atr_value = item.new_data.get(attribute['name'])
            atr_id = attribute['id']
            atr_type = attribute['type']
            if atr_type == 8 and atr_value:
                ref_atr_key = atr_value
                values = self._ref_atr_values.get(attribute['id'])
                atr_value = values.get(ref_atr_key) if values else None
                if not atr_value:
                    atr_value = self._ref_atr(value=ref_atr_key, atr=attribute)
                    self._ref_atr_values.setdefault(attribute['id'], {})
                    self._ref_atr_values[attribute['id']][ref_atr_key] = atr_value
            atr_body = {
                'Name': 'forvalidation',
                'Value': atr_value,
                'Type': atr_type,
                'Id': atr_id
            }
            request_body.append(atr_body)
        return request_body

    def create_items(self, items: list[Item]) -> dict:
        """
        Method creates all of passed items in neosintez and writes only key attribute with key value of each item.
        Against, method only creates the items and not writes all of attributes values but instead that it changes
        the status of item to "updated" for further handling
        :param items: list if Item instances to create
        :return: counter like dict with keys "success" and "error"
        """
        statistic = {'success': 0, 'error': 0}
        for item in items:
            item.self_id = self._create_in_neosintez(item.parent_id, self._config['item_class_id'], item.name)
            if item.self_id:
                request_body = [{
                    'Name': 'forvalidation',
                    'Value': item.key,
                    'Type': 2,
                    'Id': self._config['key_attribute_id']
                }]
                response = self._put_attributes(item.self_id, request_body)
                if response.status_code == 200:
                    statistic['success'] += 1
                    item.status = 'updated'
                    continue
            statistic['error'] += 1
        return statistic

    def update_items(self, items: list[Item]) -> dict:
        statistic = {'success': 0, 'error': 0}
        for item in items:
            request_body = self._get_request_body(item)
            response = self._put_attributes(item.self_id, request_body)
            if response.status_code == 200:
                statistic['success'] += 1
                continue
            statistic['error'] += 1
        return statistic

    def get_root_by_subobject_names(self, items: list[Item]) -> dict[str, str]:
        """
        Method:
        1) get all subobjects from neosintez;
        2) get their titles name;
        3) get title id inside the construction
        4) get root inside the title and return it
        :param items: list of items
        :return: dict like {subobject_name: root id}
        """
        construction_id = items[0].construction.self_id
        subobject_names = set(map(lambda x: x.subobject, items))
        result = {x: {'subobject': x} for x in subobject_names}

        all_subobjects = self._get_items_by_class(
            parent_id=self.COMMON_ATTRIBUTES_ID['subobject_list_parent_id'],
            class_id=self.COMMON_ATTRIBUTES_ID['subobject_list_class_id'],
        )
        all_subobjects = list(
            filter(
                lambda x: x['Object']['Attributes'].get(self.COMMON_ATTRIBUTES_ID['titles_attribute_id']),
                all_subobjects['Result']
            )
        )

        # get dict {subobject name: title attribute value}
        subobject_title_name = dict(map(lambda x: (x['Object']['Name'], x['Object']['Attributes'][
            self.COMMON_ATTRIBUTES_ID['titles_attribute_id']]['Value']['Id']), all_subobjects))

        for subobject in result.values():
            subobject['title_attribute_value'] = subobject_title_name.get(subobject['subobject'])

        for subobject in result:
            title_attribute_value = result[subobject]['title_attribute_value']

            if not title_attribute_value:
                continue

            title_id = self._get_id_by_key(
                parent_id=construction_id,
                class_id=self.COMMON_ATTRIBUTES_ID['title_class_id'],
                name='forvalidation',
                value=title_attribute_value,
                attribute_value_id=self.COMMON_ATTRIBUTES_ID['titles_attribute_id'],
            )

            if not title_id:
                continue

            root_id = self._get_id_by_name(
                parent_id=title_id,
                class_id=self._config['root_class_id'],
                name=self._config['root_name'],
                create=True
            )
            result[subobject]['root_id'] = root_id

        # subobject_roots_id = {subobject_name: roots_id.get(subobject_name) for subobject_name in subobject_names}
        return result

    def get_one_root_for_construction(self, construction: Construction) -> str:
        construction_id = construction.self_id
        class_id = self._config['root_for_skipped_class_id']
        root_id = self._get_id_by_name(
            parent_id=construction_id,
            class_id=class_id,
            name=self._config['root_name'],
            create=True
        )
        return root_id

    def get_group_by_group_names(self, items: list[Item]) -> dict[str, dict[str, str]]:
        """
        Method searches of groups by name inside root. If there is no group it creates a group
        :param items: list of items
        :return: dict like {root_id: {group_name: group_id}}
        """
        roots_id = set(map(lambda x: x.root_id, items))
        result = {}
        for root_id in roots_id:
            result[root_id] = {}
            items_in_root = list(filter(lambda x: x.root_id == root_id, items))
            groups = set(map(lambda x: x.group, items_in_root))
            for group in groups:
                group_id = self._get_id_by_name(
                    parent_id=root_id,
                    class_id=self._config['group_class_id'],
                    name=group,
                    create=True,
                )
                result[root_id][group] = group_id
        return result

    def get_data(self, parent_id) -> list[dict]:
        response = self._get_items_by_class(parent_id, self._config['item_class_id'])
        data = list()
        for item in response['Result']:
            # check if marked for delete
            marked_delete = item['Object']['Attributes'].get(self.COMMON_ATTRIBUTES_ID['delete_mark_attribute_id'])
            if marked_delete:
                marked_delete = marked_delete['Value'] == self.COMMON_ATTRIBUTES_ID['delete_mark_value_id']
            else:
                marked_delete = False
            item_dict = {
                'id': item['Object']['Id'],
                'delete': marked_delete
            }
            for attribute in self._mapping_data:
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
                    if isinstance(atr_value, str):
                        atr_value = atr_value.strip()
                else:
                    atr_value = None

                item_dict[name] = atr_value

            data.append(item_dict)
        return data

    def delete_item(self, item: Item) -> str:
        req_url = self.url + f'api/objects/{item.self_id}'
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {self._token}',
            'Content-Type': 'application/json-patch+json'
        }
        response = self._session.delete(req_url, headers=headers)
        if response.status_code == 200:
            status = 'success'
        else:
            status = 'error'
        return status

    def mark_as_delete(self, item: Item) -> str:
        # if the item already marked just skip it
        delete_mark = item.new_data['delete']
        if delete_mark:
            return 'success'

        attribute_value_id = self.COMMON_ATTRIBUTES_ID['delete_mark_attribute_id']
        value_id = self.COMMON_ATTRIBUTES_ID['delete_mark_value_id']
        request_body = [{
            'Name': 'forvalidation',
            'Value': {'Id': value_id, 'Name': 'forvalidation'},
            'Type': 8,
            'Id': attribute_value_id
        }]
        response = self._put_attributes(item.self_id, request_body)
        if response.status_code == 200:
            status = 'success'
        else:
            status = 'error'
        return status

    def _ref_atr(self, value, atr):
        value = value.rstrip('.')
        folder_id = atr['folder']
        class_id = atr['class']
        item_id = self._get_id_by_name(folder_id, class_id, value)
        if item_id:
            return {'Id': item_id, 'Name': 'forvalidation'}
        else:
            return None

    def total_in_neosintez(self, construction: Construction) -> int:
        parent_id = construction.self_id
        response = self._get_items_by_class(parent_id, self._config['item_class_id'], take=0)
        return response['Total']

    def close(self):
        if self._session_object:
            self._session_object.close()
