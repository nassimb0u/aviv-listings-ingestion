from fastapi.testclient import TestClient

from app.models import listing_category, listing_subcategory, listing_transaction_type

from app.core.config import settings
import random
import string


def gen_random_string(char_pool, length: int = 20) -> str:
    return "".join(random.choices(char_pool, k=length))


def test_read_listings(client: TestClient) -> None:
    params = {}
    limit = random.randint(1, 200)
    params["limit"] = limit
    subcategory = random.choice([v.name for v in list(listing_subcategory)] + [None])
    if subcategory:
        params["subcategory"] = subcategory
    category = random.choice([v.name for v in list(listing_category)] + [None])
    if category:
        params["category"] = category
    transaction_type = random.choice(
        [v.name for v in list(listing_transaction_type)] + [None]
    )
    if transaction_type:
        params["transaction_type"] = transaction_type
    city = random.choice(
        [None, gen_random_string(string.ascii_letters, random.randint(1, 20))]
    )
    if city:
        params["city"] = city
    zipcode = random.choice(
        [None, gen_random_string(string.digits, random.randint(1, 5))]
    )
    if zipcode:
        params["zipcode"] = zipcode
    min_price = random.choice([None, random.uniform(0, 5000000)])
    if min_price is not None:
        params["min_price"] = min_price
    max_price = random.choice([None, random.uniform(0, 5000000)])
    if max_price is not None:
        params["max_price"] = max_price
    min_area = random.choice([None, random.uniform(0, 30000)])
    if min_area is not None:
        params["min_area"] = min_area
    max_area = random.choice([None, random.uniform(0, 30000)])
    if max_area is not None:
        params["max_area"] = max_area
    min_build_year = random.choice([None, random.randint(0, 3000)])
    if min_build_year is not None:
        params["min_build_year"] = min_build_year
    max_build_year = random.choice([None, random.randint(0, 3000)])
    if max_build_year is not None:
        params["max_build_year"] = max_build_year
    has_build_year = random.choice([None, True, False])
    if has_build_year is not None:
        params["has_build_year"] = has_build_year
    has_garden = random.choice([None, True, False])
    if has_garden is not None:
        params["has_garden"] = has_garden
    has_passenger_lift = random.choice([None, True, False])
    if has_passenger_lift is not None:
        params["has_passenger_lift"] = has_passenger_lift
    is_new_construction = random.choice([None, True, False])
    has_cellar = random.choice([None, True, False])
    if has_cellar is not None:
        params["has_cellar"] = has_cellar
    is_furnished = random.choice([None, True, False])
    if is_furnished is not None:
        params["is_furnished"] = is_furnished
    sort_by_change_date = random.choice([None, "asc", "desc"])
    if sort_by_change_date:
        params["sort_by_change_date"] = sort_by_change_date

    r = client.get(f"{settings.API_V1_STR}/listings", params=params)
    assert r.status_code == 200
    listings = r.json()
    assert len(listings["data"]) <= limit
    assert listings["count"] >= len(listings["data"])
    for listing in listings["data"]:
        if subcategory:
            assert listing["item_subcategory"] == subcategory
        if category:
            r = client.get(
                f"{settings.API_V1_STR}/item_categories/{listing['item_subcategory']}"
            )
            assert r.status_code == 200
            item_category = r.json()
            assert item_category["category"] == category
        if transaction_type:
            assert listing["transaction_type"] == transaction_type
        if city:
            assert city.lower() in listing["city"].lower()
        if zipcode:
            assert zipcode in listing["zipcode"]
        if min_price is not None:
            assert listing["price"] >= min_price
        if max_price is not None:
            assert listing["price"] <= max_price
        if min_area is not None:
            assert listing["area"] >= min_area
        if max_area is not None:
            assert listing["area"] <= max_area
        if min_build_year is not None:
            assert listing["build_year"] >= min_build_year
        if max_build_year is not None:
            assert listing["build_year"] <= max_build_year
        if has_build_year is not None:
            if has_build_year:
                assert listing["build_year"] is not None
            else:
                assert listing["build_year"] is None
        if has_garden is not None:
            assert listing["has_garden"] == has_garden
        if has_passenger_lift is not None:
            assert listing["has_passenger_lift"] == has_passenger_lift
        if is_new_construction is not None:
            assert listing["is_new_construction"] == is_new_construction
        if has_cellar is not None:
            assert listing["has_cellar"] == has_cellar
        if is_furnished is not None:
            assert listing["is_furnished"] == is_furnished
        if sort_by_change_date == "asc":
            assert listings["data"] == sorted(
                listings["data"], key=lambda x: x["change_date"]
            )
        elif sort_by_change_date == "desc":
            assert listings["data"] == sorted(
                listings["data"], key=lambda x: x["change_date"], reverse=True
            )


def test_read_listing_by_id(client: TestClient) -> None:
    listing_id = random.randint(154794171, 190900715)
    r = client.get(f"{settings.API_V1_STR}/listings/{listing_id}")
    assert r.status_code == 200 or r.status_code == 404
    if r.status_code == 200:
        listing = r.json()
        assert listing["listing_id"] == listing_id
