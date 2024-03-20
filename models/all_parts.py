from pydantic import BaseModel


class AllParts(BaseModel):
    object: str
    id: str
    component: str
    name: str
    type_line: str
    uri: str

    def __getitem__(self, item):
        return getattr(self, item)
