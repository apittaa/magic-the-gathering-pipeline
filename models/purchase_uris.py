from pydantic import BaseModel


class PurchaseUris(BaseModel):
    tcgplayer: str
    cardmarket: str
    cardhoarder: str
    
    def __getitem__(self, item):
        return getattr(self, item)
    