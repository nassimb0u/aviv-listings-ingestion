from datetime import datetime

from sqlmodel import Field, Relationship, SQLModel
from enum import Enum


class listing_subcategory(Enum):
    SINGLE_FAMILY_HOUSE = "SINGLE_FAMILY_HOUSE"
    APARTMENT = "APARTMENT"
    EXCEPTIONAL_PROPERTY = "EXCEPTIONAL_PROPERTY"
    VILLA = "VILLA"
    DUPLEX_OR_TRIPLEX = "DUPLEX_OR_TRIPLEX"
    TOWN_HOUSE = "TOWN_HOUSE"
    FARMHOUSE = "FARMHOUSE"
    MANOR = "MANOR"
    CHALET = "CHALET"
    SEMIDETACHED_HOUSE = "SEMIDETACHED_HOUSE"
    GITE = "GITE"
    STORAGE_PRODUCTION = "STORAGE_PRODUCTION"
    PLOT = "PLOT"
    OFFICE = "OFFICE"
    PARKING = "PARKING"
    TRADING = "TRADING"
    MISCELLANEOUS = "MISCELLANEOUS"


class listing_category(Enum):
    HOUSE = "HOUSE"
    APARTMENT = "APARTMENT"
    STORAGE_PRODUCTION = "STORAGE_PRODUCTION"
    PLOT = "PLOT"
    OFFICE = "OFFICE"
    PARKING = "PARKING"
    TRADING = "TRADING"
    MISCELLANEOUS = "MISCELLANEOUS"


class listing_transaction_type(Enum):
    SELL = "SELL"
    RENT = "RENT"


class ItemCategory(SQLModel, table=True):
    __tablename__ = "item_category"
    subcategory: listing_subcategory = Field(primary_key=True)
    category: listing_category

    listings: list["Listing"] = Relationship(back_populates="item_category")


class Listing(SQLModel, table=True):
    __tablename__ = "listing"

    listing_id: int = Field(primary_key=True)
    transaction_type: listing_transaction_type
    item_subcategory: listing_subcategory = Field(
        foreign_key="item_category.subcategory"
    )
    start_date: datetime
    change_date: datetime
    price: float = Field(gt=0)
    area: float | None = Field(default=None, gt=0)
    site_area: float | None = Field(default=None, gt=0)
    floor: int | None = None
    room_count: int | None = Field(default=None, ge=0)
    balcony_count: int | None = Field(default=None, ge=0)
    terrace_count: int | None = Field(default=None, ge=0)
    has_garden: bool | None = None
    city: str = Field(max_length=255)
    zipcode: str = Field(regex=r"^\d+$", max_length=5)
    has_passenger_lift: bool | None = None
    is_new_construction: bool | None = None
    build_year: int | None = Field(default=None, ge=0)
    terrace_area: float | None = Field(default=None, gt=0)
    has_cellar: bool | None = None
    is_furnished: bool | None = None

    item_category: ItemCategory = Relationship(back_populates="listings")


# Public (read) schemas
class ItemCategoryPublic(SQLModel):
    subcategory: listing_subcategory
    category: listing_category


class ItemCategoriesPublic(SQLModel):
    data: list[ItemCategoryPublic]
    count: int


class ListingPublic(SQLModel):
    listing_id: int
    transaction_type: listing_transaction_type
    item_subcategory: listing_subcategory
    start_date: datetime
    change_date: datetime
    price: float
    area: float | None
    site_area: float | None
    floor: int | None
    room_count: int | None
    balcony_count: int | None
    terrace_count: int | None
    has_garden: bool | None
    city: str
    zipcode: str
    has_passenger_lift: bool | None
    is_new_construction: bool | None
    build_year: int | None
    terrace_area: float | None
    has_cellar: bool | None
    is_furnished: bool | None


class ListingsPublic(SQLModel):
    data: list[ListingPublic]
    count: int
