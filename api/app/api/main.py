from fastapi import APIRouter

from app.api.routes import item_categories, listings

api_router = APIRouter()
api_router.include_router(item_categories.router)
api_router.include_router(listings.router)
