from common.service import RepeatedTask, ScheduledTask

from common.performance import performance_systems
from common.api.server_api import Stats, User
from common.logging import get_logger
from common.servers import ServerAPI
from common.database.objects import *
from common.events import *

from datetime import date, timedelta
from sqlalchemy.orm import Session
from common.utils import Schedule

from typing import List, Tuple
from . import TrackerConfig

import common.repos.beatmaps as beatmaps
import common.app as app

class TrackerTask(RepeatedTask):
    
    def __init__(self, task_name: str, interval: int, config: TrackerConfig) -> None:
        self.config = config
        self.logger = get_logger(f"{config.server_api.server_name}_{task_name}")
        super().__init__(f"{config.server_api.server_name}_{task_name}", interval)


class ScheduledTrackerTask(ScheduledTask):
    
    def __init__(self, task_name: str, schedule: Schedule, config: TrackerConfig) -> None:
        self.config = config
        self.logger = get_logger(f"{config.server_api.server_name}_{task_name}")
        super().__init__(f"{config.server_api.server_name}_{task_name}", schedule)


class TrackLiveLeaderboard(TrackerTask):

    def __init__(self, config: TrackerConfig) -> None:
        super().__init__("live_lb_tracker", 60*15, config)
    
    def can_run(self) -> bool:
        if not self.config.server_api.supports_lb_tracking:
            return False
        return super().can_run()

    def run(self):
        modes = [(0,0), (1,0), (2,0), (3,0)]
        if self.config.server_api.supports_rx:
            modes.extend(((0,1), (1,1), (2,1), (0,2)))
        with app.database.managed_session() as session:
            users_updated = 0
            for mode, relax in modes:
                old_lb = session.query(DBStatsCompact).filter(DBStatsCompact.server == self.config.server_api.server_name, DBStatsCompact.leaderboard_type == "pp", DBStatsCompact.mode == mode, DBStatsCompact.relax == relax).all()
                old_lb_by_id = {}
                new_lb_by_id = {}

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
                    new_lb_by_id[user.id] = user
                    if user.id in old_lb_by_id:
                        if old_lb_by_id[user.id].play_count != stats.play_count:
                            self.process_user_update(session, user, stats, mode, relax)
                            users_updated+=1
                    if (dbuser := session.get(DBUser, (user.id, user.server))):
                        if dbuser.username != user.username: # maybe rename event?
                            dbuser.username_history.append(dbuser.username)
                            dbuser.username = user.username
                        dbuser.latest_activity = user.latest_activity
                        dbuser.country = user.country # just in case
                    else:
                        session.merge(user.to_db())
                    session.merge(stats.to_db_compact())

                missing_users = {k:v for k,v in old_lb_by_id.items() if k not in new_lb_by_id}
                
                for user in missing_users.values():
                    user_info, stats = self.config.server_api.get_user_info(user.id)
                    if not user_info or user_info.banned:
                        process_ban(self.config.server_api, session, user.id)

                session.commit()
        app.events.trigger(LeaderboardUpdateEvent(self.config.server_api.server_name, "pp", users_updated))
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

        is_akatsuki = user.server == self.config.server_api.server_name and relax > 0
        if is_akatsuki: # calc rx playtime from scratch cuz broken
            playtime = session.get(DBAkatsukiPlaytime, (user.id, mode, relax))
            if not playtime:
                playtime = DBAkatsukiPlaytime(
                    user_id = user.id,
                    mode = mode,
                    relax = relax,
                    playtime = 0
                )
                session.add(playtime)

        for score in self.config.server_api.get_user_recent(user.id, mode, relax):
            if session.query(DBScore).filter(
                DBScore.id == score.id, 
                DBScore.server == self.config.server_api.server_name
            ).first():
                break
            if not (dbmap := beatmaps.get_beatmap(score.beatmap_id)):
                self.logger.warning(f"Beatmap {score.beatmap_id} not found, can't store score {score.id}")
            else:
                if (db_most_played := session.get(DBMapPlaycount, (user.id, self.config.server_api.server_name, score.beatmap_id))):
                    db_most_played.play_count += 1
                else:
                    session.merge(DBMapPlaycount(
                        user_id = user.id,
                        server = self.config.server_api.server_name,
                        beatmap_id = score.beatmap_id,
                        play_count = 1
                    ))
                session.commit()
                session.merge(score.to_db())

                if is_akatsuki:
                    divisor = 1.5 if score.mods & 64 else 0.75 if score.mods & 256 else 1 # HT/DT
                    if score.completed > 1:
                        playtime.playtime += dbmap.hit_length / divisor
                    else:
                        playtime.playtime += (dbmap.hit_length / (score.get_total_hits()/dbmap.get_total_hits())) / divisor

                if score.completed > 2:
                    for db_score in session.query(DBScore).filter(
                        DBScore.beatmap_id == score.beatmap_id, 
                        DBScore.server == score.server,
                        DBScore.user_id == score.user_id,
                        DBScore.mode == score.mode,
                        DBScore.relax == score.relax,
                        DBScore.completed > 2
                    ):
                        if db_score.id == score.id:
                            continue
                        if db_score.completed == score.completed:
                            db_score.completed = 2
                
        for hidden_score in session.query(DBScore).filter(
            DBScore.user_id == user.id,
            DBScore.server == user.server,
            DBScore.hidden == True
        ):
            hidden_score.hidden = False
        session.merge(DBUserQueue(
            server = self.config.server_api.server_name,
            user_id = user_full.id,
            mode = mode,
            relax = relax,
            date = date.today()
        ))

        session.commit()


class TrackGenericLeaderboard(TrackerTask):

    def __init__(self, leaderboard: str, config: TrackerConfig) -> None:
        self.leaderboard = leaderboard
        super().__init__(f"live_{leaderboard}_lb_tracker", 60*30, config)
    
    def can_run(self) -> bool:
        if not self.config.server_api.supports_lb_tracking:
            return False
        return super().can_run()

    def run(self):
        modes = [(0,0), (1,0), (2,0), (3,0)]
        if self.config.server_api.supports_rx:
            modes.extend(((0,1), (1,1), (2,1), (0,2)))
        with app.database.managed_session() as session:
            for mode, relax in modes:
                old_lb = session.query(DBStatsCompact).filter(DBStatsCompact.server == self.config.server_api.server_name, DBStatsCompact.leaderboard_type == self.leaderboard, DBStatsCompact.mode == mode, DBStatsCompact.relax == relax).all()

                if old_lb:
                    for object in old_lb:
                        session.delete(object)

                live_lb: List[Tuple[User, Stats]] = list()
                page = 1

                while True:
                    try:
                        lb = self.config.server_api.get_leaderboard(mode, relax, page=page, length=500, sort=self.leaderboard)
                        if not lb:
                            break
                        for user,stats in lb:
                            if not stats.global_rank or not stats.pp:
                                break
                            live_lb.append((user, stats))
                        else:
                            page += 1
                            continue
                        break
                    except Exception:
                        self.logger.error(f"An error occurred while trying to fetch leaderboard!", exc_info=True)
                        return False
                for user, stats in live_lb:
                    if (dbuser := session.get(DBUser, (user.id, user.server))):
                        if dbuser.username != user.username: # maybe rename event?
                            dbuser.username_history.append(dbuser.username)
                            dbuser.username = user.username
                        dbuser.latest_activity = user.latest_activity
                        dbuser.country = user.country # just in case
                    else:
                        session.merge(user.to_db())
                    session.merge(stats.to_db_compact())
                # Fix country rank
                countries = {}
                for user in session.query(DBStatsCompact).filter(DBStatsCompact.server == self.config.server_api.server_name, DBStatsCompact.leaderboard_type == self.leaderboard, DBStatsCompact.mode == mode, DBStatsCompact.relax == relax).order_by(DBStatsCompact.global_rank.desc()):
                    if (dbuser := session.query(DBUser).filter(DBUser.id == user.id).first()):
                        if dbuser.country in countries:
                            countries[dbuser.country] += 1
                        else:
                            countries[dbuser.country] = 1
                        user.country_rank = countries[dbuser.country]
                session.commit()

        app.events.trigger(LeaderboardUpdateEvent(self.config.server_api.server_name, self.leaderboard, 0))
        return True

class ProcessQueue(TrackerTask):
    
    def __init__(self, config: TrackerConfig) -> None:
        super().__init__("process_queue", 60*15, config)

    def can_run(self) -> bool:
        if not self.config.server_api.supports_lb_tracking:
            return False
        return super().can_run()

    def run(self):
        with app.database.managed_session() as session:
            for queue in session.query(DBUserQueue).filter(DBUserQueue.server==self.config.server_api.server_name, DBUserQueue.date < date.today()):
                is_akatsuki = self.config.server_api.server_name == "akatsuki" and queue.relax > 0 # recalculate rx playtime cuz broken...
                user_info, stats = self.config.server_api.get_user_info(queue.user_id)
                if not user_info or user_info.banned:
                    if self.config.server_api.ping_server():
                        process_ban(self.config.server_api, session, queue.user_id)
                    else:
                        self.logger.warning("Server down?")
                    continue
                session.merge(user_info.to_db())
                for stat in stats:
                    if stat.pp == 0:
                        continue
                    stat.date = queue.date # meh
                    session.merge(stat.to_db())
                scores_count = session.query(DBScore).filter(
                    DBScore.server == self.config.server_api.server_name,
                    DBScore.mode == queue.mode,
                    DBScore.relax == queue.relax,
                    DBScore.user_id == queue.user_id,
                    DBScore.completed == 3
                ).count()
                # Assume user never got fetched if scores are under 250
                if scores_count < 250:
                    page = 1
                    while True:
                        most_played = self.config.server_api.get_user_most_played(
                            user_id = queue.user_id,
                            mode = queue.mode,
                            relax = queue.relax,
                            page = page
                        )
                        page += 1
                        if not most_played:
                            break
                        if is_akatsuki:
                            playtime = session.get(DBAkatsukiPlaytime, (queue.user_id, queue.mode, queue.relax))
                            if not playtime:
                                playtime = DBAkatsukiPlaytime(
                                    user_id = queue.user_id,
                                    mode = queue.mode,
                                    relax = queue.relax,
                                    playtime = 0
                                )
                                session.add(playtime)
                        for beatmap in most_played:
                            if not (dbmap := beatmaps.get_beatmap(beatmap.beatmap_id)):
                                continue
                            session.commit()
                            session.flush()
                            if is_akatsuki:
                                playtime.playtime += (dbmap.hit_length / (dbmap.get_total_hits() / 33.33)) * beatmap.play_count
                            if (db_most_played := session.query(DBMapPlaycount).filter(DBMapPlaycount.user_id == queue.user_id, DBMapPlaycount.beatmap_id == beatmap.beatmap_id, DBMapPlaycount.server == self.config.server_api.server_name).first()):
                                db_most_played.play_count += beatmap.play_count
                            else:
                                session.merge(beatmap.to_db())
                    page = 1
                    while True:
                        scores = self.config.server_api.get_user_best(
                            user_id = queue.user_id,
                            mode = queue.mode,
                            relax = queue.relax,
                            page = page
                        )
                        page += 1
                        if not scores:
                            break
                        for score in scores:
                            if not (dbmap := beatmaps.get_beatmap(score.beatmap_id)):
                                self.logger.warning(f"Beatmap {score.beatmap_id} not found, can't store score {score.id}")
                                continue
                            session.commit()
                            session.flush()

                            if not session.query(DBScore).filter(DBScore.id == score.id, DBScore.server == score.server).first():
                                if is_akatsuki:
                                    divisor = 1.5 if score.mods & 64 else 0.75 if score.mods & 256 else 1 # HT/DT
                                    if score.completed > 1:
                                        playtime.playtime += dbmap.hit_length / divisor
                                    else:
                                        playtime.playtime += (dbmap.hit_length / (score.get_total_hits()/dbmap.get_total_hits())) / divisor
                                session.merge(score.to_db())

                page = 1
                while True:
                    first_places, _ = self.config.server_api.get_user_1s(
                        user_id = queue.user_id,
                        mode = queue.mode,
                        relax = queue.relax,
                        page = page)
                    if not first_places:
                        break
                    for first_place in first_places:
                        if not session.query(DBScore).filter(DBScore.id == first_place.id, DBScore.server == first_place.server).first():
                            if not beatmaps.get_beatmap(first_place.beatmap_id):
                                self.logger.warning(f"Beatmap {first_place.beatmap_id} not found, can't store score {first_place.id}")
                                continue
                            else:
                                session.commit()
                                session.flush()
                                session.merge(first_place.to_db())
                        session.merge(DBFirstPlace(
                            id=first_place.id,
                            user_id=first_place.user_id,
                            server=first_place.server,
                            mode=first_place.mode,
                            relax=first_place.relax,
                            beatmap_id=first_place.beatmap_id,
                            date=date.today()
                        ))
                    page += 1
                self.logger.info(f"Processed user {user_info.username} ({user_info.id})")
                session.delete(queue)
            session.commit()
        return True

class RecalculateScores(TrackerTask):
    def __init__(self, config: TrackerConfig) -> None:
        super().__init__("recalculate_scores", 60*60*48, config)

    def run(self):
        with app.database.managed_session() as session:
            modes = [(0,0), (1,0), (2,0), (3,0)]
            if self.config.server_api.supports_rx:
                modes.extend(((0,1), (1,1), (2,1), (0,2))) 
            for mode, relax in modes:
                scores = session.query(DBScore).filter(
                    DBScore.server == self.config.server_api.server_name,
                    DBScore.mode == mode,
                    DBScore.relax == relax,
                    DBScore.pp_system != self.config.server_api.get_pp_system(mode, relax)
                ).all()
                for score in scores:
                    new_value = performance_systems[self.config.server_api.server_name].calculate_db_score(score)
                    if not new_value:
                        continue
                    self.logger.info(f"Recalculated score {score.id} {score.pp} -> {new_value}")
                    score.pp_system = self.config.server_api.get_pp_system(mode, relax)
                    score.pp = new_value
                session.commit()
        return True

class TrackClanLiveLeaderboard(TrackerTask):
    
    def __init__(self, config: TrackerConfig) -> None:
        super().__init__("track_clan_leaderboard", 60*15, config)

    def can_run(self) -> bool:
        if not self.config.server_api.supports_clans or not self.config.server_api.supports_lb_tracking:
            return False
        return super().can_run()

    def run(self):
        modes = [(0,0), (1,0), (2,0), (3,0)]
        if self.config.server_api.supports_rx:
            modes.extend(((0,1), (1,1), (2,1), (0,2)))
        with app.database.managed_session() as session:
            for mode, relax in modes:
                clans = {}
                page = 1
                while True:
                    lb = self.config.server_api.get_clan_leaderboard_1s(mode, relax, page, 100)
                    if not lb:
                        break
                    for clan, stats in lb:
                        if stats.first_places == 0:
                            page = -1
                            break
                        clans[clan.id] = (clan, stats)
                    if page == -1:
                        break
                    page += 1
                page = 1
                while True:
                    lb = self.config.server_api.get_clan_leaderboard(mode, relax, page, 100)
                    if not lb:
                        break
                    for clan, stats in lb:
                        if clan.id in clans:
                            stats.rank_1s = clans[clan.id][1].rank_1s
                            stats.first_places = clans[clan.id][1].first_places
                            clan.tag = clans[clan.id][0].tag
                            del clans[clan.id]
                        if (db_clan := session.get(DBClan, (clan.id, clan.server))):
                            db_clan.name = clan.name
                            if clan.tag:
                                db_clan.tag = clan.tag
                        else:
                            session.add(DBClan(
                                server = clan.server,
                                name = clan.name,
                                tag = clan.tag,
                                id = clan.id
                            ))
                        session.merge(stats.to_db_compact())
                    page += 1
                # Maybe iterate remaining clans? Shouldnt matter iirc since no inactivity
                session.commit()
        return True

class AutoqueueLinkedUsers(TrackerTask):
    
    def __init__(self, config:TrackerConfig):
        super().__init__("autoqueue_linked_users", 60*60, config)
    
    def can_run(self) -> bool:
        if not self.config.server_api.supports_lb_tracking:
            return False
        return super().can_run()
    
    def run(self):
        server = self.config.server_api.server_name
        with app.database.managed_session() as session:
            for link in session.query(DBBotLink):
                if server not in link.links:
                    continue
                if session.get(DBUserQueue, (link.links[server], server, link.default_mode, link.default_relax, date.today())):
                    continue
                session.add(DBUserQueue(
                    user_id = link.links[server],
                    server = server,
                    mode = link.default_mode,
                    relax = link.default_relax,
                    date = date.today()
                ))
                session.commit()
                self.logger.info(f"Autoqueued user {link.links[server]}")
        return True

class UpdateOldStats(ScheduledTrackerTask):
    
    def __init__(self, config: TrackerConfig):
        super().__init__("update_old_stats", Schedule(2, 0, 0), config)
    
    def can_run(self) -> bool:
        if not self.config.server_api.supports_lb_tracking:
            return False
        return super().can_run()
    
    def run(self):
        server = self.config.server_api.server_name
        updated = 0
        with app.database.managed_session() as session:
            old = session.query(DBStats).filter(
                DBStats.date == date.today() - timedelta(days = 1),
                DBStats.server == server
            )
            for old_stats in old:
                if session.get(DBStats, (server, old_stats.user_id, old_stats.mode, old_stats.relax, date.today())):
                    continue
                live_pp = session.get(DBStatsCompact, (old_stats.user_id, server, "pp", old_stats.mode, old_stats.relax))
                live_score = session.get(DBStatsCompact, (old_stats.user_id, server, "pp", old_stats.mode, old_stats.relax))
                global_rank, country_rank, score_rank, country_score_rank = 0,0,0,0
                if live_pp:
                    global_rank = live_pp.global_rank
                    country_rank = live_pp.country_rank
                if live_score:
                    score_rank = live_score.global_rank
                    country_score_rank = live_score.country_rank
                session.add(old_stats.update(date.today(), global_rank, country_rank, score_rank, country_score_rank))
                session.commit()
                updated += 1
        self.logger.info(f"Updated {updated} old stats".format(updated))
        return True
    
class TrackLinkedUserStats(TrackerTask):
    
    def __init__(self, config: TrackerConfig) -> None:
        super().__init__("track_linked_users", 60*15, config)
    
    def can_run(self) -> bool:
        # Ignore servers that automatically track stats
        if self.config.server_api.supports_lb_tracking:
            return False
        return super().can_run()

    def run(self):
        server_name = self.config.server_api.server_name
        yesterday = date.today() - timedelta(days = 1)
        modes = [(0,0), (1,0), (2,0), (3,0)]
        if self.config.server_api.supports_rx:
            modes.extend(((0,1), (1,1), (2,1), (0,2)))
        with app.database.managed_session() as session:
            for link in session.query(DBBotLink):
                if server_name not in link.links:
                    continue
                if session.query(DBStats).filter(
                    DBStats.server == server_name,
                    DBStats.user_id == link.links[server_name],
                    DBStats.date == yesterday
                ).count() > 0:
                    continue
                user, stats = self.config.server_api.get_user_info(link.links[server_name])
                if not user:
                    continue
                session.merge(user.to_db())
                for stat in stats:
                    if not stat.pp:
                        continue
                    session.merge(stat.to_db())
            session.commit()
        return True


def process_ban(server_api: ServerAPI, session: Session, user_id: int): 
    if server_api.ping_server():
        user = session.get(DBUser, (user_id, server_api.server_name))
        for score in session.query(DBScore).filter(
            DBScore.server == server_api.server_name,
            DBScore.user_id == user_id,
        ):
            score.hidden = True
            if user:
                user.banned = True
                session.commit()
            app.events.trigger(BannedUserEvent(user_id, server_api.server_name, user))