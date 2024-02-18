from common.service import TaskedService, Task
from tracker.tasks import *
from typing import List
from . import TrackerConfig

import common.servers as servers

def build_tasks(config: TrackerConfig) -> List[Task]:
    tasks = [TrackLiveLeaderboard(config), TrackClanLiveLeaderboard(config), ProcessQueue(config), RecalculateScores(config), TrackLinkedUserStats(config)]
    tasks.extend(config.additional_tasks)
    return tasks

class ServerTracker(TaskedService):
    
    def __init__(self, config: TrackerConfig, daemonize=False) -> None:
        super().__init__(f"{config.server_api.server_name}_tracker_svc", build_tasks(config), daemonize)

def get_services() -> List[TaskedService]:
    return [
        ServerTracker(TrackerConfig(server_api=servers.by_name("akatsuki"))),
        ServerTracker(TrackerConfig(server_api=servers.by_name("titanic"))),
        ServerTracker(TrackerConfig(server_api=servers.by_name("bancho")))
    ]
