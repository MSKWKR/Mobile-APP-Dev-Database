from dataclasses import dataclass

@dataclass
class AppVersion:
    app_db_id: int
    version: str