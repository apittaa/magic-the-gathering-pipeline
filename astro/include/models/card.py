from pydantic import BaseModel
from typing import List, Optional

from models.image_uris import ImageUris
from models.all_parts import AllParts
from models.legalities import Legalities
from models.prices import Prices
from models.related_uris import RelatedUris
from models.purchase_uris import PurchaseUris


class Card(BaseModel):
    object: str
    id: str
    oracle_id: Optional[str] = ""
    multiverse_ids: Optional[List] = None
    mtgo_id: Optional[int] = 0
    mtgo_foil_id: Optional[int] = 0
    tcgplayer_id: Optional[int] = 0
    cardmarket_id: Optional[int] = 0
    name: str
    lang: str
    released_at: str
    uri: str
    scryfall_uri: str
    layout: str
    highres_image: bool
    image_status: str
    image_uris: Optional[ImageUris] = None
    mana_cost: Optional[str] = ""
    cmc: Optional[float] = 0.0
    type_line: Optional[str] = ""
    oracle_text: Optional[str] = ""
    power: Optional[str] = ""
    toughness: Optional[str] = ""
    colors: Optional[List[str]] = None
    color_identity: List[str]
    keywords: List[str]
    all_parts: Optional[List[AllParts]] = None
    legalities: Legalities
    games: List[str]
    reserved: bool
    foil: Optional[bool] = None
    nonfoil: Optional[bool] = None
    finishes: List[str]
    oversized: bool
    promo: bool
    reprint: bool
    variation: bool
    set_id: str
    set: str
    set_name: str
    set_type: str
    set_uri: str
    set_search_uri: str
    scryfall_set_uri: str
    rulings_uri: str
    prints_search_uri: str
    collector_number: str
    digital: bool
    rarity: str
    flavor_text: Optional[str] = ""
    card_back_id: Optional[str] = ""
    artist: Optional[str] = ""
    artist_ids: Optional[List[str]] = None
    illustration_id: Optional[str] = ""
    border_color: str
    frame: str
    full_art: bool
    textless: bool
    booster: bool
    story_spotlight: bool
    edhrec_rank: Optional[int] = 0
    penny_rank: Optional[int] = 0
    prices: Prices
    related_uris: RelatedUris
    purchase_uris: Optional[PurchaseUris] = None

    def __getitem__(self, item):
        return getattr(self, item)
