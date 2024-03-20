from pydantic import BaseModel
from typing import Optional


class RelatedUris(BaseModel):
    tcgplayer_infinite_articles: Optional[str] = ""
    tcgplayer_infinite_decks: Optional[str] = ""
    edhrec: Optional[str] = ""

    def __getitem__(self, item):
        return getattr(self, item)
