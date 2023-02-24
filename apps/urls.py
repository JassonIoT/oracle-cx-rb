from fastapi import FastAPI
from apps.IntegracionesCX import urls_integracionesCX


apps =FastAPI()

apps.include_router(urls_integracionesCX.router)