from os import environ


class Config:
    REDIS_URL = environ.get('REDIS_URL', 'redis://default:bjp9MLfxGbaKtenObZEF@containers-us-west-63.railway.app:6531')

    HF_SECRET_KEY = environ.get('HF_SECRET_KEY')

    GH_SECRET_KEY = environ.get('GH_SECRET_KEY')

    REFRESH_PASSWORD = environ.get('REFRESH_PASSWORD', '123456')
