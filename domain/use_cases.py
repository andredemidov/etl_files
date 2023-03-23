import logging
from collections import Counter


class IntegrateByModeConstruction:

    def __init__(self, item_repository):
        self._item_repository = item_repository

    @staticmethod
    def _log_statistic(statistic: dict):
        message = ', '.join([f'{c[0]} - {c[1]}' for c in statistic.items()])
        if statistic.get('error'):
            logging.warning(message)
        else:
            logging.info(message)

    def execute(self):
        logging.info('Getting data')
        items = self._item_repository.get()
        statuses = [item.status for item in items]
        counter = Counter(statuses)
        self._log_statistic(counter)

        logging.info('Creating')
        create_statistic = self._item_repository.create()
        self._log_statistic(create_statistic)

        logging.info('Updating')
        update_statistic = self._item_repository.update()
        self._log_statistic(update_statistic)

        logging.info('Deleting')
        delete_statistic = self._item_repository.delete()
        self._log_statistic(delete_statistic)

        logging.info('Completed')
        total = self._item_repository.total_in_target()
        logging.info(f'Total in target system {total}')


class IntegrateByMode:
    pass


class IntegrateByConstruction:
    pass


class IntegrateAll:
    pass
