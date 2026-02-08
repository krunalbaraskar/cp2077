"""
Supabase database connection for TLE Discord bot.
Replaces SQLite with Supabase (PostgreSQL) for persistent storage.
"""
import os
import logging
from collections import namedtuple
from enum import IntEnum

from discord.ext import commands
from supabase import create_client, Client

from tle import constants
from tle.util import codeforces_api as cf, codeforces_common as cf_common

logger = logging.getLogger(__name__)

_DEFAULT_VC_RATING = 1500


class Gitgud(IntEnum):
    GOTGUD = 0
    GITGUD = 1
    NOGUD = 2
    FORCED_NOGUD = 3


class Duel(IntEnum):
    PENDING = 0
    DECLINED = 1
    WITHDRAWN = 2
    EXPIRED = 3
    ONGOING = 4
    COMPLETE = 5
    INVALID = 6


class Winner(IntEnum):
    DRAW = 0
    CHALLENGER = 1
    CHALLENGEE = 2


class DuelType(IntEnum):
    UNOFFICIAL = 0
    OFFICIAL = 1


class RatedVC(IntEnum):
    ONGOING = 0
    FINISHED = 1


class ParticipantStatus(IntEnum):
    INVITED = 0
    ACCEPTED = 1
    DECLINED = 2


class UserDbError(commands.CommandError):
    pass


class DatabaseDisabledError(UserDbError):
    pass


class DummyUserDbConn:
    def __getattribute__(self, item):
        raise DatabaseDisabledError


class UniqueConstraintFailed(UserDbError):
    pass


def _make_row(data, fields=None):
    """Convert dict to namedtuple-like object."""
    if data is None:
        return None
    if fields:
        Row = namedtuple('Row', fields)
        return Row(*[data.get(f) for f in fields])
    Row = namedtuple('Row', data.keys())
    return Row(*data.values())


class SupabaseDbConn:
    """Supabase database connection - drop-in replacement for UserDbConn."""
    
    def __init__(self):
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")
        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY environment variables required")
        self.client: Client = create_client(url, key)
        logger.info("Connected to Supabase")

    # ==================== Handle Methods ====================

    def set_handle(self, user_id, guild_id, handle):
        # Check if handle already exists for different user
        existing = self.client.table('user_handle').select('user_id').eq(
            'guild_id', str(guild_id)
        ).eq('handle', handle).execute()
        
        if existing.data and str(existing.data[0]['user_id']) != str(user_id):
            raise UniqueConstraintFailed
        
        result = self.client.table('user_handle').upsert({
            'user_id': str(user_id),
            'guild_id': str(guild_id),
            'handle': handle,
            'active': True
        }).execute()
        return len(result.data)

    def get_handle(self, user_id, guild_id):
        result = self.client.table('user_handle').select('handle').eq(
            'user_id', str(user_id)
        ).eq('guild_id', str(guild_id)).execute()
        return result.data[0]['handle'] if result.data else None

    def get_user_id(self, handle, guild_id):
        result = self.client.table('user_handle').select('user_id').ilike(
            'handle', handle
        ).eq('guild_id', str(guild_id)).execute()
        return int(result.data[0]['user_id']) if result.data else None

    def remove_handle(self, handle, guild_id):
        result = self.client.table('user_handle').delete().ilike(
            'handle', handle
        ).eq('guild_id', str(guild_id)).execute()
        return len(result.data)

    def get_handles_for_guild(self, guild_id):
        result = self.client.table('user_handle').select('user_id, handle').eq(
            'guild_id', str(guild_id)
        ).eq('active', True).execute()
        return [(int(r['user_id']), r['handle']) for r in result.data]

    def set_inactive(self, guild_id_user_id_pairs):
        count = 0
        for guild_id, user_id in guild_id_user_id_pairs:
            result = self.client.table('user_handle').update({
                'active': False
            }).eq('guild_id', str(guild_id)).eq('user_id', str(user_id)).execute()
            count += len(result.data)
        return count

    # ==================== CF User Cache ====================

    def cache_cf_user(self, user):
        data = {
            'handle': user[0],
            'first_name': user[1],
            'last_name': user[2],
            'country': user[3],
            'city': user[4],
            'organization': user[5],
            'contribution': user[6],
            'rating': user[7],
            'max_rating': user[8],
            'last_online_time': user[9],
            'registration_time': user[10],
            'friend_of_count': user[11],
            'title_photo': user[12]
        }
        result = self.client.table('cf_user_cache').upsert(data).execute()
        return len(result.data)

    def fetch_cf_user(self, handle):
        result = self.client.table('cf_user_cache').select('*').ilike(
            'handle', handle
        ).execute()
        if not result.data:
            return None
        r = result.data[0]
        user_tuple = (
            r['handle'], r['first_name'], r['last_name'], r['country'],
            r['city'], r['organization'], r['contribution'], r['rating'],
            r['max_rating'], r['last_online_time'], r['registration_time'],
            r['friend_of_count'], r['title_photo']
        )
        return cf_common.fix_urls(cf.User._make(user_tuple))

    def get_cf_users_for_guild(self, guild_id):
        # Join user_handle with cf_user_cache
        handles_result = self.client.table('user_handle').select(
            'user_id, handle'
        ).eq('guild_id', str(guild_id)).eq('active', True).execute()
        
        users = []
        for h in handles_result.data:
            cf_user = self.fetch_cf_user(h['handle'])
            if cf_user:
                users.append((int(h['user_id']), cf_user))
        return users

    # ==================== Duelist Methods ====================

    def register_duelist(self, user_id):
        result = self.client.table('duelist').upsert({
            'user_id': user_id,
            'rating': 1500
        }, on_conflict='user_id').execute()
        return len(result.data)

    def get_duel_rating(self, user_id):
        result = self.client.table('duelist').select('rating').eq(
            'user_id', user_id
        ).execute()
        return result.data[0]['rating'] if result.data else None

    def set_duel_rating(self, user_id, rating):
        result = self.client.table('duelist').update({
            'rating': rating
        }).eq('user_id', user_id).execute()
        return len(result.data)

    def get_duelists(self):
        result = self.client.table('duelist').select('user_id, rating').execute()
        return [(r['user_id'], r['rating']) for r in result.data]

    # ==================== 1v1 Duel Methods ====================

    def check_duel_challenge(self, userid):
        result = self.client.table('duel').select('id').or_(
            f'challenger.eq.{userid},challengee.eq.{userid}'
        ).in_('status', [Duel.ONGOING, Duel.PENDING]).execute()
        return _make_row(result.data[0], ['id']) if result.data else None

    def check_duel_accept(self, challengee):
        result = self.client.table('duel').select(
            'id, challenger, problem_name'
        ).eq('challengee', challengee).eq('status', Duel.PENDING).execute()
        return _make_row(result.data[0], ['id', 'challenger', 'problem_name']) if result.data else None

    def check_duel_decline(self, challengee):
        result = self.client.table('duel').select('id, challenger').eq(
            'challengee', challengee
        ).eq('status', Duel.PENDING).execute()
        return _make_row(result.data[0], ['id', 'challenger']) if result.data else None

    def check_duel_withdraw(self, challenger):
        result = self.client.table('duel').select('id, challengee').eq(
            'challenger', challenger
        ).eq('status', Duel.PENDING).execute()
        return _make_row(result.data[0], ['id', 'challengee']) if result.data else None

    def check_duel_draw(self, userid):
        result = self.client.table('duel').select(
            'id, challenger, challengee, start_time'
        ).or_(f'challenger.eq.{userid},challengee.eq.{userid}').eq(
            'status', Duel.ONGOING
        ).execute()
        return _make_row(result.data[0], ['id', 'challenger', 'challengee', 'start_time']) if result.data else None

    def check_duel_complete(self, userid):
        result = self.client.table('duel').select(
            'id, challenger, challengee, start_time, problem_name, contest_id, p_index, type, nohandicap'
        ).or_(f'challenger.eq.{userid},challengee.eq.{userid}').eq(
            'status', Duel.ONGOING
        ).execute()
        if not result.data:
            return None
        r = result.data[0]
        return _make_row(r, ['id', 'challenger', 'challengee', 'start_time', 
                             'problem_name', 'contest_id', 'p_index', 'type', 'nohandicap'])

    def create_duel(self, challenger, challengee, issue_time, prob, dtype, nohandicap=0):
        result = self.client.table('duel').insert({
            'challenger': challenger,
            'challengee': challengee,
            'issue_time': issue_time,
            'problem_name': prob.name,
            'contest_id': prob.contestId,
            'p_index': prob.index,
            'status': Duel.PENDING,
            'type': dtype,
            'nohandicap': nohandicap
        }).execute()
        return result.data[0]['id'] if result.data else 0

    def cancel_duel(self, duelid, status):
        result = self.client.table('duel').update({
            'status': status
        }).eq('id', duelid).execute()
        return len(result.data)

    def invalidate_duel(self, duelid):
        return self.cancel_duel(duelid, Duel.INVALID)

    def start_duel(self, duelid, start_time):
        result = self.client.table('duel').update({
            'start_time': start_time,
            'status': Duel.ONGOING
        }).eq('id', duelid).eq('status', Duel.PENDING).execute()
        return len(result.data)

    def complete_duel(self, duelid, winner, finish_time, winner_id=None, loser_id=None, 
                      delta=None, dtype=None):
        update_data = {
            'finish_time': finish_time,
            'status': Duel.COMPLETE,
            'winner': winner
        }
        if dtype is not None:
            update_data['type'] = dtype
        result = self.client.table('duel').update(update_data).eq('id', duelid).execute()
        return len(result.data)

    def get_duel_status(self, duelid):
        result = self.client.table('duel').select('status').eq('id', duelid).execute()
        return result.data[0]['status'] if result.data else None

    def get_pair_duels(self, usera, userb):
        result = self.client.table('duel').select('*').or_(
            f'and(challenger.eq.{usera},challengee.eq.{userb}),and(challenger.eq.{userb},challengee.eq.{usera})'
        ).eq('status', Duel.COMPLETE).execute()
        return [_make_row(r) for r in result.data]

    def get_duel_wins(self, userid):
        as_challenger = self.client.table('duel').select('id', count='exact').eq(
            'challenger', userid
        ).eq('status', Duel.COMPLETE).eq('winner', Winner.CHALLENGER).execute()
        
        as_challengee = self.client.table('duel').select('id', count='exact').eq(
            'challengee', userid
        ).eq('status', Duel.COMPLETE).eq('winner', Winner.CHALLENGEE).execute()
        
        return (as_challenger.count or 0) + (as_challengee.count or 0)

    def get_duel_losses(self, userid):
        as_challenger = self.client.table('duel').select('id', count='exact').eq(
            'challenger', userid
        ).eq('status', Duel.COMPLETE).eq('winner', Winner.CHALLENGEE).execute()
        
        as_challengee = self.client.table('duel').select('id', count='exact').eq(
            'challengee', userid
        ).eq('status', Duel.COMPLETE).eq('winner', Winner.CHALLENGER).execute()
        
        return (as_challenger.count or 0) + (as_challengee.count or 0)

    def get_duel_draws(self, userid):
        result = self.client.table('duel').select('id', count='exact').or_(
            f'challenger.eq.{userid},challengee.eq.{userid}'
        ).eq('status', Duel.COMPLETE).eq('winner', Winner.DRAW).execute()
        return result.count or 0

    def get_duel_history(self, userid):
        result = self.client.table('duel').select('*').or_(
            f'challenger.eq.{userid},challengee.eq.{userid}'
        ).eq('status', Duel.COMPLETE).order('finish_time', desc=True).execute()
        return [_make_row(r) for r in result.data]

    def get_recent_duels(self):
        result = self.client.table('duel').select('*').eq(
            'status', Duel.COMPLETE
        ).order('finish_time', desc=True).limit(10).execute()
        return [_make_row(r) for r in result.data]

    def get_ongoing_duels(self):
        result = self.client.table('duel').select('*').in_(
            'status', [Duel.PENDING, Duel.ONGOING]
        ).execute()
        return [_make_row(r) for r in result.data]

    # ==================== Challenge/Gitgud Methods ====================

    def new_challenge(self, user_id, issue_time, prob, delta):
        # Insert challenge
        challenge_result = self.client.table('challenge').insert({
            'user_id': str(user_id),
            'issue_time': issue_time,
            'problem_name': prob.name,
            'contest_id': prob.contestId,
            'p_index': prob.index,
            'rating_delta': delta,
            'status': Gitgud.GITGUD
        }).execute()
        
        if not challenge_result.data:
            return 0
        
        challenge_id = challenge_result.data[0]['id']
        
        # Ensure user_challenge exists
        self.client.table('user_challenge').upsert({
            'user_id': str(user_id),
            'score': 0,
            'num_completed': 0,
            'num_skipped': 0
        }, on_conflict='user_id').execute()
        
        # Update only if no active challenge
        existing = self.client.table('user_challenge').select(
            'active_challenge_id'
        ).eq('user_id', str(user_id)).execute()
        
        if existing.data and existing.data[0]['active_challenge_id'] is not None:
            return 0
        
        self.client.table('user_challenge').update({
            'active_challenge_id': challenge_id,
            'issue_time': issue_time
        }).eq('user_id', str(user_id)).execute()
        
        return 1

    def check_challenge(self, user_id):
        uc = self.client.table('user_challenge').select(
            'active_challenge_id, issue_time'
        ).eq('user_id', str(user_id)).execute()
        
        if not uc.data or uc.data[0]['active_challenge_id'] is None:
            return None
        
        c_id = uc.data[0]['active_challenge_id']
        issue_time = uc.data[0]['issue_time']
        
        challenge = self.client.table('challenge').select(
            'problem_name, contest_id, p_index, rating_delta'
        ).eq('id', c_id).execute()
        
        if not challenge.data:
            return None
        
        c = challenge.data[0]
        return (c_id, issue_time, c['problem_name'], c['contest_id'], 
                c['p_index'], c['rating_delta'])

    def complete_challenge(self, user_id, challenge_id, finish_time, delta):
        # Update challenge status
        self.client.table('challenge').update({
            'finish_time': finish_time,
            'status': Gitgud.GOTGUD
        }).eq('id', challenge_id).eq('status', Gitgud.GITGUD).execute()
        
        # Update user stats
        uc = self.client.table('user_challenge').select('score, num_completed').eq(
            'user_id', str(user_id)
        ).execute()
        
        if uc.data:
            self.client.table('user_challenge').update({
                'score': uc.data[0]['score'] + delta,
                'num_completed': uc.data[0]['num_completed'] + 1,
                'active_challenge_id': None,
                'issue_time': None
            }).eq('user_id', str(user_id)).execute()
        
        return 1

    def skip_challenge(self, user_id, challenge_id, status):
        self.client.table('user_challenge').update({
            'active_challenge_id': None,
            'issue_time': None
        }).eq('user_id', str(user_id)).execute()
        
        self.client.table('challenge').update({
            'status': status
        }).eq('id', challenge_id).eq('status', Gitgud.GITGUD).execute()
        
        return 1

    def get_gudgitters(self):
        result = self.client.table('user_challenge').select('user_id, score').execute()
        return [(r['user_id'], r['score']) for r in result.data]

    def howgud(self, user_id):
        result = self.client.table('challenge').select('rating_delta').eq(
            'user_id', str(user_id)
        ).not_.is_('finish_time', 'null').execute()
        return [(r['rating_delta'],) for r in result.data]

    def get_noguds(self, user_id):
        result = self.client.table('challenge').select('problem_name').eq(
            'user_id', str(user_id)
        ).eq('status', Gitgud.NOGUD).execute()
        return {r['problem_name'] for r in result.data}

    def gitlog(self, user_id):
        result = self.client.table('challenge').select(
            'issue_time, finish_time, problem_name, contest_id, p_index, rating_delta, status'
        ).eq('user_id', str(user_id)).neq(
            'status', Gitgud.FORCED_NOGUD
        ).order('issue_time', desc=True).execute()
        return [_make_row(r, ['issue_time', 'finish_time', 'problem_name', 
                              'contest_id', 'p_index', 'rating_delta', 'status']) 
                for r in result.data]

    # ==================== Reminder Methods ====================

    def get_reminder_settings(self, guild_id):
        result = self.client.table('reminder').select(
            'channel_id, role_id, before'
        ).eq('guild_id', str(guild_id)).execute()
        return _make_row(result.data[0], ['channel_id', 'role_id', 'before']) if result.data else None

    def set_reminder_settings(self, guild_id, channel_id, role_id, before):
        self.client.table('reminder').upsert({
            'guild_id': str(guild_id),
            'channel_id': str(channel_id) if channel_id else None,
            'role_id': str(role_id) if role_id else None,
            'before': before
        }).execute()

    def clear_reminder_settings(self, guild_id):
        self.client.table('reminder').delete().eq('guild_id', str(guild_id)).execute()

    # ==================== Rankup Methods ====================

    def get_rankup_channel(self, guild_id):
        result = self.client.table('rankup').select('channel_id').eq(
            'guild_id', str(guild_id)
        ).execute()
        return int(result.data[0]['channel_id']) if result.data else None

    def set_rankup_channel(self, guild_id, channel_id):
        self.client.table('rankup').upsert({
            'guild_id': str(guild_id),
            'channel_id': str(channel_id)
        }).execute()

    def clear_rankup_channel(self, guild_id):
        self.client.table('rankup').delete().eq('guild_id', str(guild_id)).execute()

    # ==================== Auto Role ====================

    def enable_auto_role_update(self, guild_id):
        self.client.table('auto_role_update').upsert({
            'guild_id': str(guild_id)
        }).execute()

    def disable_auto_role_update(self, guild_id):
        self.client.table('auto_role_update').delete().eq(
            'guild_id', str(guild_id)
        ).execute()

    def has_auto_role_update_enabled(self, guild_id):
        result = self.client.table('auto_role_update').select('guild_id').eq(
            'guild_id', str(guild_id)
        ).execute()
        return bool(result.data)

    def get_auto_role_update_guilds(self):
        result = self.client.table('auto_role_update').select('guild_id').execute()
        return [r['guild_id'] for r in result.data]

    # ==================== Starboard Methods ====================

    def get_starboard_entry(self, guild_id, emoji):
        cfg = self.client.table('starboard_config_v1').select('channel_id').eq(
            'guild_id', str(guild_id)
        ).eq('emoji', emoji).execute()
        
        if not cfg.data:
            return None
        
        emo = self.client.table('starboard_emoji_v1').select('threshold, color').eq(
            'guild_id', str(guild_id)
        ).eq('emoji', emoji).execute()
        
        if not emo.data:
            return None
        
        return (int(cfg.data[0]['channel_id']), int(emo.data[0]['threshold']), 
                int(emo.data[0]['color']))

    def add_starboard_emoji(self, guild_id, emoji, threshold, color):
        self.client.table('starboard_emoji_v1').upsert({
            'guild_id': str(guild_id),
            'emoji': emoji,
            'threshold': threshold,
            'color': color
        }).execute()
        return 1

    def remove_starboard_emoji(self, guild_id, emoji):
        result = self.client.table('starboard_emoji_v1').delete().eq(
            'guild_id', str(guild_id)
        ).eq('emoji', emoji).execute()
        return len(result.data)

    def update_starboard_threshold(self, guild_id, emoji, threshold):
        result = self.client.table('starboard_emoji_v1').update({
            'threshold': threshold
        }).eq('guild_id', str(guild_id)).eq('emoji', emoji).execute()
        return len(result.data)

    def update_starboard_color(self, guild_id, emoji, color):
        result = self.client.table('starboard_emoji_v1').update({
            'color': color
        }).eq('guild_id', str(guild_id)).eq('emoji', emoji).execute()
        return len(result.data)

    def set_starboard_channel(self, guild_id, emoji, channel_id):
        self.client.table('starboard_config_v1').upsert({
            'guild_id': str(guild_id),
            'emoji': emoji,
            'channel_id': str(channel_id)
        }).execute()
        return 1

    def clear_starboard_channel(self, guild_id, emoji):
        result = self.client.table('starboard_config_v1').delete().eq(
            'guild_id', str(guild_id)
        ).eq('emoji', emoji).execute()
        return len(result.data)

    def add_starboard_message(self, original_msg_id, starboard_msg_id, guild_id, emoji):
        self.client.table('starboard_message_v1').insert({
            'original_msg_id': str(original_msg_id),
            'starboard_msg_id': str(starboard_msg_id),
            'guild_id': str(guild_id),
            'emoji': emoji
        }).execute()

    def check_exists_starboard_message(self, original_msg_id, emoji):
        result = self.client.table('starboard_message_v1').select('original_msg_id').eq(
            'original_msg_id', str(original_msg_id)
        ).eq('emoji', emoji).execute()
        return bool(result.data)

    def remove_starboard_message(self, *, original_msg_id=None, emoji=None, starboard_msg_id=None):
        if original_msg_id is not None and emoji is not None:
            result = self.client.table('starboard_message_v1').delete().eq(
                'original_msg_id', str(original_msg_id)
            ).eq('emoji', emoji).execute()
        elif starboard_msg_id is not None:
            result = self.client.table('starboard_message_v1').delete().eq(
                'starboard_msg_id', str(starboard_msg_id)
            ).execute()
        else:
            return 0
        return len(result.data)

    # ==================== Multiplayer Duel Methods ====================

    def create_multiplayer_duel(self, creator_id, guild_id, issue_time, num_problems, 
                                 rating, dtype, nohandicap=0):
        result = self.client.table('multiplayer_duel').insert({
            'creator_id': creator_id,
            'guild_id': str(guild_id),
            'issue_time': issue_time,
            'num_problems': num_problems,
            'rating': rating,
            'status': Duel.PENDING,
            'type': dtype,
            'nohandicap': nohandicap
        }).execute()
        return result.data[0]['id'] if result.data else 0

    def add_multiplayer_participant(self, duel_id, user_id, status=ParticipantStatus.INVITED):
        self.client.table('multiplayer_duel_participant').upsert({
            'duel_id': duel_id,
            'user_id': user_id,
            'status': status
        }).execute()

    def add_multiplayer_problem(self, duel_id, problem, order):
        self.client.table('multiplayer_duel_problem').insert({
            'duel_id': duel_id,
            'problem_name': problem.name,
            'contest_id': problem.contestId,
            'p_index': problem.index,
            'problem_order': order
        }).execute()

    def get_multiplayer_duel(self, duel_id):
        result = self.client.table('multiplayer_duel').select('*').eq(
            'id', duel_id
        ).execute()
        return _make_row(result.data[0]) if result.data else None

    def get_multiplayer_duel_by_participant(self, user_id, status=None):
        query = self.client.table('multiplayer_duel_participant').select(
            'duel_id'
        ).eq('user_id', user_id)
        
        if status is not None:
            query = query.eq('status', status)
        
        participant = query.execute()
        if not participant.data:
            return None
        
        duel_id = participant.data[0]['duel_id']
        return self.get_multiplayer_duel(duel_id)

    def get_pending_multiplayer_invite(self, user_id):
        # Find duels where user is invited and duel is pending
        result = self.client.table('multiplayer_duel_participant').select(
            'duel_id'
        ).eq('user_id', user_id).eq('status', ParticipantStatus.INVITED).execute()
        
        for r in result.data:
            duel = self.client.table('multiplayer_duel').select('*').eq(
                'id', r['duel_id']
            ).eq('status', Duel.PENDING).execute()
            if duel.data:
                return (r['duel_id'], duel.data[0]['creator_id'])
        return None

    def accept_multiplayer_invite(self, duel_id, user_id):
        self.client.table('multiplayer_duel_participant').update({
            'status': ParticipantStatus.ACCEPTED
        }).eq('duel_id', duel_id).eq('user_id', user_id).execute()

    def decline_multiplayer_invite(self, duel_id, user_id):
        self.client.table('multiplayer_duel_participant').update({
            'status': ParticipantStatus.DECLINED
        }).eq('duel_id', duel_id).eq('user_id', user_id).execute()

    def get_multiplayer_participants(self, duel_id):
        result = self.client.table('multiplayer_duel_participant').select('*').eq(
            'duel_id', duel_id
        ).execute()
        return [_make_row(r) for r in result.data]

    def get_multiplayer_problems(self, duel_id):
        result = self.client.table('multiplayer_duel_problem').select('*').eq(
            'duel_id', duel_id
        ).order('problem_order').execute()
        return [_make_row(r) for r in result.data]

    def start_multiplayer_duel(self, duel_id, start_time):
        self.client.table('multiplayer_duel').update({
            'start_time': start_time,
            'status': Duel.ONGOING
        }).eq('id', duel_id).execute()

    def cancel_multiplayer_duel(self, duel_id, status):
        self.client.table('multiplayer_duel').update({
            'status': status
        }).eq('id', duel_id).execute()

    def complete_multiplayer_duel(self, duel_id, finish_time):
        self.client.table('multiplayer_duel').update({
            'finish_time': finish_time,
            'status': Duel.COMPLETE
        }).eq('id', duel_id).execute()

    def update_multiplayer_participant(self, duel_id, user_id, problems_solved=None, 
                                        total_time=None, placement=None, rating_delta=None):
        data = {}
        if problems_solved is not None:
            data['problems_solved'] = problems_solved
        if total_time is not None:
            data['total_time'] = total_time
        if placement is not None:
            data['placement'] = placement
        if rating_delta is not None:
            data['rating_delta'] = rating_delta
        
        if data:
            self.client.table('multiplayer_duel_participant').update(data).eq(
                'duel_id', duel_id
            ).eq('user_id', user_id).execute()

    def get_user_active_multiplayer_duel(self, user_id):
        result = self.client.table('multiplayer_duel_participant').select(
            'duel_id'
        ).eq('user_id', user_id).execute()
        
        for r in result.data:
            duel = self.client.table('multiplayer_duel').select('*').eq(
                'id', r['duel_id']
            ).in_('status', [Duel.PENDING, Duel.ONGOING]).execute()
            if duel.data:
                return _make_row(duel.data[0])
        return None

    def get_multiplayer_duel_history(self, user_id):
        result = self.client.table('multiplayer_duel_participant').select(
            'duel_id, problems_solved, total_time, placement, rating_delta'
        ).eq('user_id', user_id).execute()
        
        history = []
        for r in result.data:
            duel = self.client.table('multiplayer_duel').select('*').eq(
                'id', r['duel_id']
            ).eq('status', Duel.COMPLETE).execute()
            if duel.data:
                history.append({
                    **duel.data[0],
                    'problems_solved': r['problems_solved'],
                    'total_time': r['total_time'],
                    'placement': r['placement'],
                    'rating_delta': r['rating_delta']
                })
        return history

    # ==================== Rated VC Methods ====================

    def create_rated_vc(self, contest_id, start_time, guild_id):
        result = self.client.table('rated_vcs').insert({
            'contest_id': contest_id,
            'start_time': start_time,
            'status': RatedVC.ONGOING,
            'guild_id': str(guild_id)
        }).execute()
        return result.data[0]['id'] if result.data else 0

    def get_rated_vc(self, vc_id):
        result = self.client.table('rated_vcs').select('*').eq('id', vc_id).execute()
        return _make_row(result.data[0]) if result.data else None

    def finish_rated_vc(self, vc_id, finish_time):
        self.client.table('rated_vcs').update({
            'finish_time': finish_time,
            'status': RatedVC.FINISHED
        }).eq('id', vc_id).execute()

    def get_ongoing_rated_vc(self, guild_id):
        result = self.client.table('rated_vcs').select('*').eq(
            'guild_id', str(guild_id)
        ).eq('status', RatedVC.ONGOING).execute()
        return _make_row(result.data[0]) if result.data else None

    def add_rated_vc_user(self, vc_id, user_id, rating):
        self.client.table('rated_vc_users').upsert({
            'vc_id': vc_id,
            'user_id': str(user_id),
            'rating': rating
        }).execute()

    def get_rated_vc_users(self, vc_id):
        result = self.client.table('rated_vc_users').select('user_id, rating').eq(
            'vc_id', vc_id
        ).execute()
        return [(r['user_id'], r['rating']) for r in result.data]

    def set_rated_vc_channel(self, guild_id, channel_id):
        self.client.table('rated_vc_settings').upsert({
            'guild_id': str(guild_id),
            'channel_id': str(channel_id)
        }).execute()

    def get_rated_vc_channel(self, guild_id):
        result = self.client.table('rated_vc_settings').select('channel_id').eq(
            'guild_id', str(guild_id)
        ).execute()
        return int(result.data[0]['channel_id']) if result.data else None

    def clear_rated_vc_channel(self, guild_id):
        self.client.table('rated_vc_settings').delete().eq(
            'guild_id', str(guild_id)
        ).execute()

    def get_ongoing_rated_vc_ids(self):
        """Get IDs of all ongoing rated VCs."""
        result = self.client.table('rated_vcs').select('id').eq(
            'status', RatedVC.ONGOING
        ).execute()
        return [r['id'] for r in result.data]

    def get_rated_vc_user_ids(self, vc_id):
        """Get user IDs participating in a rated VC."""
        result = self.client.table('rated_vc_users').select('user_id').eq(
            'vc_id', vc_id
        ).execute()
        return [r['user_id'] for r in result.data]

    def update_vc_rating(self, vc_id, user_id, rating):
        """Update a user's rating in a rated VC."""
        self.client.table('rated_vc_users').upsert({
            'vc_id': vc_id,
            'user_id': str(user_id),
            'rating': rating
        }).execute()

    def get_vc_rating(self, user_id, default_if_not_exist=True):
        """Get a user's current VC rating."""
        result = self.client.table('rated_vc_users').select(
            'vc_id, rating'
        ).eq('user_id', str(user_id)).not_.is_(
            'rating', 'null'
        ).order('vc_id', desc=True).limit(1).execute()
        
        if result.data:
            return result.data[0]['rating']
        if default_if_not_exist:
            return _DEFAULT_VC_RATING
        return None

    def get_vc_rating_history(self, user_id):
        """Get VC rating history for a user."""
        result = self.client.table('rated_vc_users').select(
            'vc_id, rating'
        ).eq('user_id', str(user_id)).not_.is_(
            'rating', 'null'
        ).execute()
        return [_make_row(r, ['vc_id', 'rating']) for r in result.data]

    def remove_last_ratedvc_participation(self, user_id):
        """Remove a user's last rated VC participation."""
        # Get the last VC
        last_result = self.client.table('rated_vc_users').select('vc_id').eq(
            'user_id', str(user_id)
        ).order('vc_id', desc=True).limit(1).execute()
        
        if not last_result.data:
            return 0
        
        vc_id = last_result.data[0]['vc_id']
        result = self.client.table('rated_vc_users').delete().eq(
            'user_id', str(user_id)
        ).eq('vc_id', vc_id).execute()
        return len(result.data)

