from pydantic import BaseModel, Field
from typing import Any, Dict, List, Union
from datetime import datetime

class Event(BaseModel):
    topic: str = Field(min_length=1)
    event_id: str = Field(min_length=1)
    timestamp: datetime
    source: str = Field(min_length=1)
    payload: Dict[str, Any]

PublishBody = Union[Event, List[Event]]
