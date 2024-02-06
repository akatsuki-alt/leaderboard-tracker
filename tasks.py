from common.database.objects import DBStatsCompact
from common.events import LeaderboardUpdateEvent
from common.api.server_api import Stats, User
from common.service import RepeatedTask
from common.logging import get_logger
from typing import List, Tuple
from . import TrackerConfig

import common.app as app

class TrackerTask(RepeatedTask):
    
    def __init__(self, task_name: str, interval: int, config: TrackerConfig) -> None:
        self.config = config
        super().__init__(f"{config.server_api.server_name}_{task_name}", interval)


class TrackLiveLeaderboard(TrackerTask):

    def __init__(self, config: TrackerConfig) -> None:
        super().__init__("live_lb_tracker", 60*15, config)
        self.logger = get_logger(self.task_name)
    
    def run(self):
        modes = [(0,0), (1,0), (2,0), (3,0)]
        if self.config.server_api.supports_rx:
            modes.extend(((0,1), (1,1), (2,1), (0,2)))
        with app.database.session as session:
            for mode, relax in modes:
                old_lb = session.query(DBStatsCompact).filter(DBStatsCompact.server == self.config.server_api.server_name, DBStatsCompact.leaderboard_type == "pp", DBStatsCompact.mode == mode, DBStatsCompact.relax == relax).all()
                if old_lb:
                    for object in old_lb:
                        session.delete(object)
                live_lb: List[Tuple[User, Stats]] = list()
                page = 1

                while True:
                    try:
                        lb = self.config.server_api.get_leaderboard(mode, relax, page=page, length=500)
                        if not lb:
                            break
                        for user,stats in lb:
                            if not stats.global_rank:
                                break
                            live_lb.append((user, stats))
                        else:
                            page += 1
                            continue
                        break
                    except Exception as e:
                        self.logger.error(f"An error occurred while trying to fetch leaderboard!", exc_info=True)
                        return False
                for user, stats in live_lb:
                    session.add(stats.to_db_compact())
                session.commit()
        app.events.trigger(LeaderboardUpdateEvent(self.config.server_api.server_name))
        return True
