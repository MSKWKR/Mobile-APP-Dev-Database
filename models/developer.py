from dataclasses import dataclass
from typing import Optional

@dataclass
class Developer:
    name: str
    store: str
    developer_url: str
    email: Optional[str] = None
    website: Optional[str] = None
    country: Optional[str] = None