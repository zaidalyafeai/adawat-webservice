from os import environ


class Config:
    REDIS_URL = environ.get('REDIS_URL', 'redis://localhost:6379')

    HF_SECRET_KEY = environ.get('HF_SECRET_KEY')