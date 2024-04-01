from pydantic import BaseModel
from typing import Optional


class Prices(BaseModel):
    usd: Optional[str] = ""
    usd_foil: Optional[str] = ""
    usd_etched: Optional[str] = ""
    eur: Optional[str] = ""
    eur_foil: Optional[str] = ""
    tix: Optional[str] = ""

    def __getitem__(self, item):
        return getattr(self, item)
