from dataclasses import dataclass


@dataclass
class Construction:

    self_id: str
    key: str
    name: str

    def __str__(self):
        return self.self_id

#
# @dataclass
# class Title:
#
#     construction: Construction
#     self_id: str
#     name: str
#
#     def __str__(self):
#         return self.self_id


@dataclass
class Item:

    item_type: str
    new_data: dict
    name: str
    construction: Construction = None
    subobject: str = None
    key: str = None
    status: str = None
    group: str = None
    parent_id: str = None
    root_id: str = None
    self_id: str = None
    delete: bool = False
