from .entities import Item, Construction


class ConstructionRepository:

    def __init__(self, target_adapter, entries: list[Construction] = None):
        self._entries = []
        self._target_adapter = target_adapter
        if entries:
            self._entries.extend(entries)

    def get(self):
        if not self._entries:
            self._get_from_source()
        return self._entries.copy()

    def _get_from_source(self):
        items = self._target_adapter.get_constructions()
        self._entries.extend(items)


class ItemRepository:

    def __init__(
            self,
            construction: Construction,
            target_adapter,
            input_adapter,
            key_column_name: str,
            subobject_column_name: str,
            name_column_name: str,
            mode: str,
            group_by_column_name: str,
            save_skipped: bool = False,
            one_root_mode: bool = False,
            entries: list[Item] = None
    ):
        self._construction = construction
        self._entries: list[Item] = []
        self._target_adapter = target_adapter
        self._input_adapter = input_adapter
        self._key_column_name = key_column_name
        self._subobject_column_name = subobject_column_name
        self._name_column_name = name_column_name
        self._mode = mode
        self._group_by_column_name = group_by_column_name
        self._new_data = None
        self._current_data = None
        self._save_skipped = save_skipped
        self._one_root_mode = one_root_mode
        if entries:
            self._entries.extend(entries)

    def get(self, *status: str, exclude=False) -> list[Item]:
        """
        Method initially gets data from data sources using adapters and initiates Item instances. Further calling just
        returns stored data
        :param status: to filter the returned list of Items by all passed statuses
        :param exclude: to invert filtering by status
        :return: list of stored Items instances
        """
        if not self._entries:
            self._get_new_data()
            items = [Item(
                construction=self._construction,
                item_type=self._mode,
                new_data=one_data,
                name=one_data[self._name_column_name],
                subobject=one_data[self._subobject_column_name],
                key=one_data[self._key_column_name],
                group=one_data[self._group_by_column_name] if self._group_by_column_name else "Прочее"
            ) for one_data in self._new_data]
            self._entries.extend(items)
            self._get_status()
            self._get_roots()

        if status and exclude:
            return list(filter(lambda x: x.status not in status, self._entries))
        elif status:
            return list(filter(lambda x: x.status in status, self._entries))

        return self._entries.copy()

    def add(self, *args):
        for item in args:
            if isinstance(item, Item):
                self._entries.append(item)
            else:
                raise TypeError('only Item instances can be in the repository')

    def create(self) -> dict[str, int]:
        items_for_create = self.get('new')
        if self._save_skipped:
            items_for_create.extend(self.get('skip'))
        self._get_parents(items=items_for_create)
        statistic_create = self._target_adapter.create_items(items_for_create)
        return statistic_create

    def update(self) -> dict[str, int]:
        statistic_update = self._target_adapter.update_items(self.get('updated'))
        return statistic_update

    def _get_new_data(self) -> list:
        if not self._new_data:
            self._new_data = self._input_adapter.get_data(self._construction.key)
        return self._new_data
        # items = [Item(self._construction, mode, data) for data in input_data]
        # self._entries.extend(items)

    def _get_current_data(self) -> list:
        if not self._current_data:
            self._current_data = self._target_adapter.get_data(self._construction.self_id)
        return self._current_data

    def _get_status(self):
        current_data = self._get_current_data()
        # order list by delete attribute to keep ones not deleted first
        # to ensure that if there is not deleted one among duplicates this one will be saved
        current_data.sort(key=lambda x: x['delete'])
        # create dict like {key: item}. If there is a duplicate drop it on the delete list
        current_dict = {}
        current_items_for_delete = []
        for current_item in current_data:
            if current_item[self._key_column_name] in current_dict:
                # duplicate detected
                # add it to the repository with delete status
                item = Item(
                    construction=self._construction,
                    item_type=self._mode,
                    new_data=current_item,
                    name='forvalidation',
                    self_id=current_item['id'],
                    status='delete'
                )
                current_items_for_delete.append(item)
            else:
                current_dict[current_item[self._key_column_name]] = current_item
        # match new data with current
        for new_item in self._entries:
            # if a match exists, remove it from the current_dict and take the id to update
            match_item = current_dict.pop(new_item.key, None)
            if match_item:
                new_item.self_id = match_item['id']
                match_item_for_compare = match_item.copy()
                del match_item_for_compare['id']
                new_item_for_compare = new_item.new_data.copy()
                if 'Папка' in new_item_for_compare:
                    del new_item_for_compare['Папка']
                if new_item_for_compare == match_item_for_compare:
                    new_item.status = 'up_to_date'
                else:
                    new_item.status = 'updated'
            else:
                new_item.status = 'new'
        # the presence of an item in current_dict means that it should be deleted
        # add it to the repository with delete status
        for current_item in current_dict.values():
            item = Item(
                construction=self._construction,
                item_type=self._mode,
                new_data=current_item,
                name='forvalidation',
                self_id=current_item['id'],
                status='delete'
            )
            current_items_for_delete.append(item)
        self.add(*current_items_for_delete)

    def _get_roots(self):
        not_deleted = self.get('new')
        if not_deleted:
            if self._one_root_mode:
                self._get_one_root(not_deleted)
            else:
                subobjects_roots = self._target_adapter.get_root_by_subobject_names(not_deleted)
                for item in not_deleted:
                    subobject_data = subobjects_roots.get(item.subobject)
                    item.root_id = subobject_data.get('root_id') if subobject_data else None
                    if not item.root_id:
                        item.status = 'skip'
                if self._save_skipped:
                    self._get_one_root(self.get('skip'))

    def _get_one_root(self, items: list[Item]):
        if items:
            one_root_id = self._target_adapter.get_one_root_for_construction(self._construction)
            for item in items:
                item.root_id = one_root_id

    def _get_parents(self, items: list[Item]):
        if items:
            if self._group_by_column_name:
                # groups must be a dict like {root_id: {group_name: group_id}}
                groups = self._target_adapter.get_group_by_group_names(items)
                for item in items:
                    item.parent_id = groups[item.root_id][item.group]
            else:
                for item in items:
                    item.parent_id = item.root_id

    def total_in_target(self) -> int:
        return self._target_adapter.total_in_neosintez(self._construction)

    def delete(self) -> dict:
        """
        Method removes instances with 'delete' status on the target system.
        :return: a counter dict with keys "success" and "error"
        """
        statistic = {'success': 0, 'error': 0}
        for item in self.get('delete'):
            status = self._target_adapter.mark_as_delete(item)
            # status = self._target_adapter.delete_item(item)
            statistic[status] += 1
        return statistic
