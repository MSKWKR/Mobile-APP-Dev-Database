from dataclasses import dataclass
from typing import Optional

@dataclass
class Developer:
    name: str
    email: Optional[str] = None
    website: Optional[str] = None