from pydantic import BaseModel


class Legalities(BaseModel):
    standard: str
    future: str
    historic: str
    timeless: str
    gladiator: str
    pioneer: str
    explorer: str
    modern: str
    legacy: str
    pauper: str
    vintage: str
    penny: str
    commander: str
    oathbreaker: str
    standardbrawl: str
    brawl: str
    alchemy: str
    paupercommander: str
    duel: str
    oldschool: str
    premodern: str
    predh: str
    
    def __getitem__(self, item):
        return getattr(self, item)
    