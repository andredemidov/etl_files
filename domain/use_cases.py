import logging
from collections import Counter


class IntegrateByModeConstruction:

    def __init__(self, item_repository):
        self._item_repository = item_repository

    @staticmethod
    def _log_statistic(statistic: dict):
        logging.info(', '.join([f'{c[0]}- {c[1]}' for c in statistic.items()]))

    def execute(self):
        items = self._item_repository.get()
        statuses = [item.status for item in items]
        counter = Counter(statuses)
        self._log_statistic(counter)

        create_statistic = self._item_repository.create()
        self._log_statistic(create_statistic)

        update_statistic = self._item_repository.update()
        self._log_statistic(update_statistic)


class IntegrateByMode:
    pass


class IntegrateByConstruction:
    pass


class IntegrateAll:
    pass
