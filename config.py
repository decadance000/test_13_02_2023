
# Конфиг PostgreSQL базы данных
DB_PG_HOST = ""
DB_PG_PORT = ""
DB_PG_NAME = ""
DB_PG_USERNAME = ""
DB_PG_PASSWORD = ""

# engine строка для sqlalchemy
DB_PG_CONN = f"postgresql://{DB_PG_USERNAME}:{DB_PG_PASSWORD}@{DB_PG_HOST}:{DB_PG_PORT}/{DB_PG_NAME}"
