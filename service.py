from common.service import TaskedService, Task
from common.api.server_api import ServerAPI
from dataclasses import dataclass
from typing import List

import common.servers as servers

@dataclass
class TrackerConfig:
    server_api: ServerAPI
    additional_tasks: List[Task] = ()

    def build_tasks(self) -> List[Task]:
        return self.additional_tasks

class ServerTracker(TaskedService):
    
    def __init__(self, config: TrackerConfig, daemonize=False) -> None:
        super().__init__(f"{config.server_api.server_name}_tracker_svc", config.build_tasks(), daemonize)

def get_services() -> List[TaskedService]:
    return [
        ServerTracker(TrackerConfig(server_api=servers.by_name("akatsuki")))
    ]
