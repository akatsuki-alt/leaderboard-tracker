from common.database.objects import DBStatsCompact, DBUser, DBScore
from common.api.server_api import Stats, User
from common.service import RepeatedTask
from common.logging import get_logger
from common.events import *

from sqlalchemy.orm import Session
from typing import List, Tuple
from . import TrackerConfig

import common.repos.beatmaps as beatmaps
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
            users_updated = 0
            for mode, relax in modes:
                old_lb = session.query(DBStatsCompact).filter(DBStatsCompact.server == self.config.server_api.server_name, DBStatsCompact.leaderboard_type == "pp", DBStatsCompact.mode == mode, DBStatsCompact.relax == relax).all()
                old_lb_by_id = {}
                if old_lb:
                    for object in old_lb:
                        old_lb_by_id[object.id] = object
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
                    if user.id in old_lb_by_id:
                        if old_lb_by_id[user.id].play_count != stats.play_count:
                            self.process_user_update(session, user, stats, mode, relax)
                            users_updated+=1
                    session.merge(stats.to_db_compact())
                session.commit()
        app.events.trigger(LeaderboardUpdateEvent(self.config.server_api.server_name, users_updated))
        return True

    def process_user_update(self, session: Session, user: User, stats: Stats, mode: int, relax: int) -> None:
        user_full, stats_full = self.config.server_api.get_user_info(user.id)
        if not user_full or not stats_full:
            return
        if not (dbuser := session.get(DBUser, (user.id, user.server))):
            app.events.trigger(NewUserDiscoveredEvent(user_full))
        for stats in stats_full:
            if stats.pp == 0:
                continue
            session.merge(stats.to_db())
        session.merge(user_full.to_db())
        for score in self.config.server_api.get_user_recent(user.id, mode, relax):
            if session.query(DBScore).filter(
                DBScore.id == score.id, 
                DBScore.server == self.config.server_api.server_name
            ).first():
                break
            if not beatmaps.get_beatmap(score.beatmap_id):
                self.logger.warning(f"Beatmap {score.beatmap_id} not found, can't store score {score.id}")
            else:
                session.merge(score.to_db())
            # TODO: Fix old statuses when changes
        session.commit()