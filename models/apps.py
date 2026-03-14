from dataclasses import dataclass
from typing import Optional

@dataclass
class App:
    developer_id: int
    store: str
    app_id: int
    app_name: str
    category: Optional[str] = None