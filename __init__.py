from common.api.server_api import ServerAPI
from dataclasses import dataclass
from common.service import Task

from typing import List

@dataclass
class TrackerConfig:
    server_api: ServerAPI
    additional_tasks: List[Task] = ()