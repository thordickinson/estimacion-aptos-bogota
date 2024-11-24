import os

class Config:
    POSTGIS_DB = os.getenv('POSTGIS_DB', 'postgis_db')
    POSTGIS_USER = os.getenv('POSTGIS_USER', 'postgres')
    POSTGIS_PASSWORD = os.getenv('POSTGIS_PASSWORD', 'password')
    POSTGIS_HOST = os.getenv('POSTGIS_HOST', 'localhost')
    POSTGIS_PORT = os.getenv('POSTGIS_PORT', '5432')

    SQLALCHEMY_DATABASE_URI = (
        f"postgresql://{POSTGIS_USER}:{POSTGIS_PASSWORD}@{POSTGIS_HOST}:{POSTGIS_PORT}/{POSTGIS_DB}"
    )
