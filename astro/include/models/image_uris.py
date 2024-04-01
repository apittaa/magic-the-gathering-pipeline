from pydantic import BaseModel


class ImageUris(BaseModel):
    small: str
    normal: str
    large: str
    png: str
    art_crop: str
    border_crop: str

    def __getitem__(self, item):
        return getattr(self, item)
