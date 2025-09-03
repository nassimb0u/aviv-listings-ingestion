from fastapi.testclient import TestClient

from app.models import listing_category, listing_subcategory

from app.core.config import settings
import random


def test_read_item_categories(client: TestClient) -> None:
    params = {}
    limit = random.randint(1, 20)
    params["limit"] = limit
    category = random.choice([v.name for v in list(listing_category)] + [None])
    if category:
        params["category"] = category
    r = client.get(
        f"{settings.API_V1_STR}/item_categories",
        params=params,
    )
    assert r.status_code == 200
    item_categories = r.json()
    if category:
        for item in item_categories["data"]:
            assert item["category"] == category
    assert len(item_categories["data"]) <= limit
    assert item_categories["count"] >= len(item_categories["data"])

    category = listing_category.HOUSE.name.lower()
    params["category"] = category
    r = client.get(
        f"{settings.API_V1_STR}/item_categories",
        params={"category": category},
    )
    assert r.status_code == 422


def test_read_item_category_by_id(client: TestClient) -> None:
    subcategory = random.choice([v.name for v in list(listing_subcategory)])
    r = client.get(f"{settings.API_V1_STR}/item_categories/{subcategory}")
    assert r.status_code == 200
    item_category = r.json()
    assert item_category["subcategory"] == subcategory

    category = item_category["category"]
    r = client.get(
        f"{settings.API_V1_STR}/item_categories", params={"category": category}
    )
    assert r.status_code == 200
    item_categories = r.json()
    subcategory_found = False
    for item in item_categories["data"]:
        if item["subcategory"] == subcategory:
            subcategory_found = True
            break
    assert subcategory_found

    subcategory = listing_category.HOUSE.name.lower()
    r = client.get(f"{settings.API_V1_STR}/item_categories/{subcategory}")
    assert r.status_code == 422
