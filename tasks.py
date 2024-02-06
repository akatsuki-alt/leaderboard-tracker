from common.service import RepeatedTask
from . import TrackerConfig

import common.app as app

class TrackerTask(RepeatedTask):
    
    def __init__(self, task_name: str, interval: int, config: TrackerConfig) -> None:
        self.config = config
        super().__init__(f"{config.server_api.server_name}_{task_name}", interval)
    

class TrackLiveLeaderboard(TrackerTask):

    def __init__(self, config: TrackerConfig) -> None:
        super().__init__("live_lb_tracker", 60*15, config)
    
    def run(self):
        with app.database.session as session:
            pass
        return False
