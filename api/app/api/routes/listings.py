from typing import Any

from fastapi import APIRouter, HTTPException
from sqlmodel import col, func, select

from app.api.deps import SessionDep
from app.models import (
    Listing,
    ListingPublic,
    ListingsPublic,
    listing_category,
    listing_subcategory,
    listing_transaction_type,
    ItemCategory,
)

router = APIRouter(prefix="/listings", tags=["listings"])


@router.get("/", response_model=ListingsPublic)
def read_listings(
    *,
    session: SessionDep,
    skip: int = 0,
    limit: int = 100,
    subcategory: listing_subcategory | None = None,
    category: listing_category | None = None,
    transaction_type: listing_transaction_type | None = None,
    city: str | None = None,
    zipcode: str | None = None,
    min_price: float | None = None,
    max_price: float | None = None,
    min_area: float | None = None,
    max_area: float | None = None,
    min_build_year: int | None = None,
    max_build_year: int | None = None,
    has_build_year: bool | None = None,
    has_garden: bool | None = None,
    has_passenger_lift: bool | None = None,
    is_new_construction: bool | None = None,
    has_cellar: bool | None = None,
    is_furnished: bool | None = None,
    sort_by_change_date: str | None = None,
) -> Any:
    """
    Retrieve listings with filtering, sorting, and pagination.
    """
    statement = select(Listing)
    count_statement = select(func.count()).select_from(Listing)

    if subcategory:
        statement = statement.where(col(Listing.item_subcategory) == subcategory)
        count_statement = count_statement.where(
            col(Listing.item_subcategory) == subcategory
        )
    if category:
        statement = statement.join(ItemCategory).where(
            col(ItemCategory.category) == category
        )
        count_statement = count_statement.join(ItemCategory).where(
            col(ItemCategory.category) == category
        )
    if transaction_type:
        statement = statement.where(col(Listing.transaction_type) == transaction_type)
        count_statement = count_statement.where(
            col(Listing.transaction_type) == transaction_type
        )
    if city:
        statement = statement.where(func.lower(Listing.city).contains(city.lower()))
        count_statement = count_statement.where(
            func.lower(Listing.city).contains(city.lower())
        )
    if zipcode:
        statement = statement.where(Listing.zipcode.contains(zipcode))
        count_statement = count_statement.where(Listing.zipcode.contains(zipcode))
    if min_price is not None:
        statement = statement.where(col(Listing.price) >= min_price)
        count_statement = count_statement.where(col(Listing.price) >= min_price)
    if max_price is not None:
        statement = statement.where(col(Listing.price) <= max_price)
        count_statement = count_statement.where(col(Listing.price) <= max_price)
    if min_area is not None:
        statement = statement.where(col(Listing.area) >= min_area)
        count_statement = count_statement.where(col(Listing.area) >= min_area)
    if max_area is not None:
        statement = statement.where(col(Listing.area) <= max_area)
        count_statement = count_statement.where(col(Listing.area) <= max_area)
    if min_build_year is not None:
        statement = statement.where(col(Listing.build_year) >= min_build_year)
        count_statement = count_statement.where(
            col(Listing.build_year) >= min_build_year
        )
    if max_build_year is not None:
        statement = statement.where(col(Listing.build_year) <= max_build_year)
        count_statement = count_statement.where(
            col(Listing.build_year) <= max_build_year
        )
    if has_build_year is not None:
        if has_build_year:
            statement = statement.where(Listing.build_year.is_not(None))
            count_statement = count_statement.where(Listing.build_year.is_not(None))
        else:
            statement = statement.where(Listing.build_year.is_(None))
            count_statement = count_statement.where(Listing.build_year.is_(None))
    if has_garden is not None:
        statement = statement.where(col(Listing.has_garden) == has_garden)
        count_statement = count_statement.where(col(Listing.has_garden) == has_garden)
    if has_passenger_lift is not None:
        statement = statement.where(
            col(Listing.has_passenger_lift) == has_passenger_lift
        )
        count_statement = count_statement.where(
            col(Listing.has_passenger_lift) == has_passenger_lift
        )
    if is_new_construction is not None:
        statement = statement.where(
            col(Listing.is_new_construction) == is_new_construction
        )
        count_statement = count_statement.where(
            col(Listing.is_new_construction) == is_new_construction
        )
    if has_cellar is not None:
        statement = statement.where(col(Listing.has_cellar) == has_cellar)
        count_statement = count_statement.where(col(Listing.has_cellar) == has_cellar)
    if is_furnished is not None:
        statement = statement.where(col(Listing.is_furnished) == is_furnished)
        count_statement = count_statement.where(
            col(Listing.is_furnished) == is_furnished
        )

    if sort_by_change_date:
        if sort_by_change_date.lower() == "asc":
            statement = statement.order_by(col(Listing.change_date).asc())
        elif sort_by_change_date.lower() == "desc":
            statement = statement.order_by(col(Listing.change_date).desc())

    count = session.exec(count_statement).one()

    listings = session.exec(statement.offset(skip).limit(limit)).all()

    return ListingsPublic(data=listings, count=count)


@router.get("/{listing_id}", response_model=ListingPublic)
def read_listing_by_id(listing_id: int, *, session: SessionDep) -> Any:
    """
    Get a specific listing by id.
    """
    listing = session.get(Listing, listing_id)
    if not listing:
        raise HTTPException(status_code=404, detail="Listing not found")
    return listing
