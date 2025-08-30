from typing import Any

from fastapi import APIRouter, HTTPException
from sqlmodel import col, func, select

from app.api.deps import SessionDep

from app.models import (
    ItemCategory,
    ItemCategoryPublic,
    ItemCategoriesPublic,
    listing_subcategory,
    listing_category,
)

router = APIRouter(prefix="/item_categories", tags=["item_categories"])


@router.get(
    "/",
    response_model=ItemCategoriesPublic,
)
def read_item_categories(
    session: SessionDep,
    skip: int = 0,
    limit: int = 100,
    category: listing_category | None = None,
) -> Any:
    """
    Retrieve item categories.
    """
    statement = select(ItemCategory)
    count_statement = select(func.count()).select_from(ItemCategory)

    if category:
        statement = statement.where(col(ItemCategory.category) == category)
        count_statement = count_statement.where(col(ItemCategory.category) == category)

    count = session.exec(count_statement).one()

    statement = statement.offset(skip).limit(limit)
    item_categories = session.exec(statement).all()

    return ItemCategoriesPublic(data=item_categories, count=count)


@router.get("/{subcategory}", response_model=ItemCategoryPublic)
def read_item_category_by_id(
    subcategory: listing_subcategory, session: SessionDep
) -> Any:
    """
    Get a specific item category by id.
    """
    item_category = session.get(ItemCategory, subcategory)
    if not item_category:
        raise HTTPException(status_code=404, detail="ItemCategory not found")
    return item_category
