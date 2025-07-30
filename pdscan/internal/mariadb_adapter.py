from .sql_adapter import SQLAdapter

class MariaDBAdapter(SQLAdapter):
    """Adapter cho MariaDB, kế thừa từ SQLAdapter (MySQL)."""
    def __init__(self, url: str, config: dict = None):
        super().__init__(url, config) 