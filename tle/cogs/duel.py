import asyncio
import datetime
import random
import re
from collections import defaultdict, namedtuple

import discord
from discord.ext import commands
from matplotlib import pyplot as plt

from tle import constants
from tle.util import (
    codeforces_api as cf,
    codeforces_common as cf_common,
    discord_common,
    graph_common as gc,
    paginator,
    table,
)
from tle.util.db.user_db_conn import Duel, DuelType, Winner, ParticipantStatus

_DUEL_INVALIDATE_TIME = 2 * 60
_DUEL_EXPIRY_TIME = 5 * 60
_DUEL_RATING_DELTA = -400
_DUEL_NO_DRAW_TIME = 10 * 60
_ELO_CONSTANT = 60
_HANDICAP_SECONDS_PER_100_RATING = 30  # 30 seconds handicap per 100 rating difference

DuelRank = namedtuple('Rank', 'low high title title_abbr color_graph color_embed')

DUEL_RANKS = (
    DuelRank(-(10**9), 1300, 'Newbie', 'N', '#CCCCCC', 0x808080),
    DuelRank(1300, 1400, 'Pupil', 'P', '#77FF77', 0x008000),
    DuelRank(1400, 1500, 'Specialist', 'S', '#77DDBB', 0x03A89E),
    DuelRank(1500, 1600, 'Expert', 'E', '#AAAAFF', 0x0000FF),
    DuelRank(1600, 1700, 'Candidate Master', 'CM', '#FF88FF', 0xAA00AA),
    DuelRank(1700, 1800, 'Master', 'M', '#FFCC88', 0xFF8C00),
    DuelRank(1800, 1900, 'International Master', 'IM', '#FFBB55', 0xF57500),
    DuelRank(1900, 2000, 'Grandmaster', 'GM', '#FF7777', 0xFF3030),
    DuelRank(2000, 2100, 'International Grandmaster', 'IGM', '#FF3333', 0xFF0000),
    DuelRank(2100, 10**9, 'Legendary Grandmaster', 'LGM', '#AA0000', 0xCC0000),
)


def rating2rank(rating):
    for rank in DUEL_RANKS:
        if rank.low <= rating < rank.high:
            return rank


class DuelCogError(commands.CommandError):
    pass


def elo_prob(player, opponent):
    return (1 + 10 ** ((opponent - player) / 400)) ** -1


def elo_delta(player, opponent, win):
    return _ELO_CONSTANT * (win - elo_prob(player, opponent))


def get_cf_user(userid, guild_id):
    handle = cf_common.user_db.get_handle(userid, guild_id)
    return cf_common.user_db.fetch_cf_user(handle)


def parse_cf_problem_url(url):
    """Parse a Codeforces problem URL and return (contest_id, problem_index).
    Returns None if the URL is not a valid CF problem URL.
    Supports:
    - https://codeforces.com/contest/123/problem/A
    - https://codeforces.com/problemset/problem/123/A
    - https://codeforces.com/gym/123/problem/A
    """
    patterns = [
        r'codeforces\.com/contest/(\d+)/problem/([A-Za-z0-9]+)',
        r'codeforces\.com/problemset/problem/(\d+)/([A-Za-z0-9]+)',
        r'codeforces\.com/gym/(\d+)/problem/([A-Za-z0-9]+)',
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return int(match.group(1)), match.group(2).upper()
    return None



def calculate_handicap(player_rating, opponent_rating):
    """Calculate time handicap in seconds for the lower rated player.
    Returns (player_handicap, opponent_handicap) where handicap is subtracted from solve time.
    The higher rated player gets 0 handicap, lower rated gets bonus time.
    """
    rating_diff = player_rating - opponent_rating
    if rating_diff > 0:
        # Player is higher rated, opponent gets handicap
        handicap = (rating_diff / 100) * _HANDICAP_SECONDS_PER_100_RATING
        return 0, handicap
    elif rating_diff < 0:
        # Opponent is higher rated, player gets handicap
        handicap = (-rating_diff / 100) * _HANDICAP_SECONDS_PER_100_RATING
        return handicap, 0
    return 0, 0


def apply_handicap(solve_time, handicap):
    """Apply handicap by subtracting from solve time (lower time = better)."""
    return max(0, solve_time - handicap)


def complete_duel(
    duelid, guild_id, win_status, winner, loser, finish_time, score, dtype
):
    winner_r = cf_common.user_db.get_duel_rating(winner.id)
    loser_r = cf_common.user_db.get_duel_rating(loser.id)
    delta = round(elo_delta(winner_r, loser_r, score))
    rc = cf_common.user_db.complete_duel(
        duelid, win_status, finish_time, winner.id, loser.id, delta, dtype
    )
    if rc == 0:
        raise DuelCogError('Hey! No cheating!')

    if dtype == DuelType.UNOFFICIAL:
        return None

    winner_cf = get_cf_user(winner.id, guild_id)
    loser_cf = get_cf_user(loser.id, guild_id)
    desc = (
        f'Rating change after **[{winner_cf.handle}]({winner_cf.url})**'
        f' vs **[{loser_cf.handle}]({loser_cf.url})**:'
    )
    embed = discord_common.cf_color_embed(description=desc)
    embed.add_field(
        name=f'{winner.display_name}',
        value=f'{winner_r} -> {winner_r + delta}',
        inline=False,
    )
    embed.add_field(
        name=f'{loser.display_name}',
        value=f'{loser_r} -> {loser_r - delta}',
        inline=False,
    )
    return embed


def check_if_allow_self_register(ctx):
    if not constants.ALLOW_DUEL_SELF_REGISTER:
        raise DuelCogError('Self Registration is not enabled.')
    return True


class Dueling(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.converter = commands.MemberConverter()
        self.draw_offers = {}

    @commands.group(brief='Duel commands', invoke_without_command=True)
    async def duel(self, ctx):
        """Duel system for competitive problem solving.
        
        **Features:**
        • 1v1 duels with ELO rating system
        • Multi-player duels (2-10 players, 1-5 problems)
        • Time handicap for lower-rated players
        • Official/unofficial duel modes
        
        **Handicap System:**
        Lower-rated players get 30s time bonus per 100 rating difference.
        Use 'nohandicap' to disable this for fair matches.
        
        **Rating:**
        Starts at 1500. Changes based on ELO after official duels.
        """
        await ctx.send_help(ctx.command)

    @duel.command(brief='Register a duelist')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def register(self, ctx, member: discord.Member):
        """Register a duelist"""
        rc = cf_common.user_db.register_duelist(member.id)
        if rc == 0:
            raise DuelCogError(f'{member.mention} is already a registered duelist')
        await ctx.send(f'{member.mention} successfully registered as a duelist.')

    @duel.command(brief='Register yourself as a duelist')
    @commands.check(check_if_allow_self_register)
    async def selfregister(self, ctx):
        """Register yourself as a duelist"""
        if not cf_common.user_db.get_handle(ctx.author.id, ctx.guild.id):
            raise DuelCogError(
                f'{ctx.author.mention}, you cannot register yourself'
                ' as a duelist without setting your handle.'
            )
        rc = cf_common.user_db.register_duelist(ctx.author.id)
        if rc == 0:
            raise DuelCogError(f'{ctx.author.mention} is already a registered duelist')
        await ctx.send(f'{ctx.author.mention} successfully registered as a duelist')

    @duel.command(
        brief='Challenge to a 1v1 duel', usage='opponent [rating] [+tag..] [~tag..] [nohandicap] [problem_url]'
    )
    async def challenge(self, ctx, opponent: discord.Member, *args):
        """Challenge another server member to a 1v1 duel.
        
        **Arguments:**
        • opponent - The user to challenge
        • rating - Problem difficulty (default: lowest CF rating - 400)
        • +tag - Include problems with this tag
        • ~tag - Exclude problems with this tag
        • nohandicap - Disable time handicap
        • problem_url - Direct Codeforces problem link (makes duel unofficial)
        
        **Handicap:**
        Lower-rated players get 30s bonus per 100 rating diff.
        Example: 1400 vs 1600 = 60s advantage for 1400.
        
        **Official vs Unofficial:**
        Duel is unofficial (no rating change) if:
        • Custom problem URL is provided
        • Problem rating < min CF rating of both players
        
        **Examples:**
        `;duel challenge @user` - Standard duel
        `;duel challenge @user 1500` - 1500-rated problem
        `;duel challenge @user +dp` - DP problems only
        `;duel challenge @user nohandicap` - No handicap
        `;duel challenge @user https://codeforces.com/contest/1/problem/A` - Specific problem
        """
        challenger_id = ctx.author.id
        challengee_id = opponent.id

        await cf_common.resolve_handles(
            ctx, self.converter, ('!' + str(ctx.author), '!' + str(opponent))
        )
        userids = [challenger_id, challengee_id]
        handles = [
            cf_common.user_db.get_handle(userid, ctx.guild.id) for userid in userids
        ]
        submissions = [await cf.user.status(handle=handle) for handle in handles]

        if not cf_common.user_db.is_duelist(challenger_id):
            raise DuelCogError(
                f'{ctx.author.mention}, you are not a registered duelist!'
            )
        if not cf_common.user_db.is_duelist(challengee_id):
            raise DuelCogError(f'{opponent.mention} is not a registered duelist!')
        if challenger_id == challengee_id:
            raise DuelCogError(f'{ctx.author.mention}, you cannot challenge yourself!')
        if cf_common.user_db.check_duel_challenge(challenger_id):
            raise DuelCogError(f'{ctx.author.mention}, you are currently in a duel!')
        if cf_common.user_db.check_duel_challenge(challengee_id):
            raise DuelCogError(f'{opponent.mention} is currently in a duel!')

        # Check for nohandicap option
        args_list = list(args)
        nohandicap = False
        for arg in args_list[:]:
            if arg.lower() == 'nohandicap':
                nohandicap = True
                args_list.remove(arg)
        
        # Check for custom problem URL
        custom_problem = None
        for arg in args_list[:]:
            parsed = parse_cf_problem_url(arg)
            if parsed:
                args_list.remove(arg)
                contest_id, problem_index = parsed
                # Look up problem in cache
                for prob in cf_common.cache2.problem_cache.problems:
                    if prob.contestId == contest_id and prob.index == problem_index:
                        custom_problem = prob
                        break
                if custom_problem is None:
                    raise DuelCogError(
                        f'Problem not found in cache. Make sure the problem exists and try again.'
                    )
                break
        
        args = tuple(args_list)

        tags = cf_common.parse_tags(args, prefix='+')
        bantags = cf_common.parse_tags(args, prefix='~')
        rating = cf_common.parse_rating(args)
        users = [cf_common.user_db.fetch_cf_user(handle) for handle in handles]
        lowest_rating = min(user.rating or 0 for user in users)
        min_cf_rating = round(lowest_rating, -2)  # Min CF rating rounded to nearest 100
        suggested_rating = max(min_cf_rating + _DUEL_RATING_DELTA, 500)
        rating = round(rating, -2) if rating else suggested_rating
        
        # Only custom problem URL makes duel unofficial
        unofficial = custom_problem is not None
        dtype = DuelType.UNOFFICIAL if unofficial else DuelType.OFFICIAL

        # If custom problem is provided, use it directly
        if custom_problem:
            problem = custom_problem
            rstr = f'{problem.rating} rated ' if problem.rating else ''
        else:
            # Random problem selection
            solved = {
                sub.problem.name
                for subs in submissions
                for sub in subs
                if sub.verdict != 'COMPILATION_ERROR'
            }
            seen = {
                name
                for userid in userids
                for (name,) in cf_common.user_db.get_duel_problem_names(userid)
            }

            def get_problems(rating):
                return [
                    prob
                    for prob in cf_common.cache2.problem_cache.problems
                    if prob.rating == rating
                    and prob.name not in solved
                    and prob.name not in seen
                    and not any(
                        cf_common.is_contest_writer(prob.contestId, handle)
                        for handle in handles
                    )
                    and not cf_common.is_nonstandard_problem(prob)
                    and prob.matches_all_tags(tags)
                    and not prob.matches_any_tag(bantags)
                ]

            for problems in map(get_problems, range(rating, 400, -100)):
                if problems:
                    break

            rstr = f'{rating} rated ' if rating else ''
            if not problems:
                raise DuelCogError(
                    f'No unsolved {rstr} problems left for'
                    f' {ctx.author.mention} vs {opponent.mention}.'
                )

            problems.sort(
                key=lambda problem: cf_common.cache2.contest_cache.get_contest(
                    problem.contestId
                ).startTimeSeconds
            )

            choice = max(random.randrange(len(problems)) for _ in range(2))
            problem = problems[choice]

        issue_time = datetime.datetime.now().timestamp()
        duelid = cf_common.user_db.create_duel(
            challenger_id, challengee_id, issue_time, problem, dtype, nohandicap
        )


        # Get duel ratings for display
        challenger_r = cf_common.user_db.get_duel_rating(challenger_id)
        challengee_r = cf_common.user_db.get_duel_rating(challengee_id)
        
        ostr = 'an **unofficial**' if unofficial else 'an **official**'
        handicap_str = ''
        if not nohandicap:
            challenger_hc, challengee_hc = calculate_handicap(challenger_r, challengee_r)
            if challenger_hc > 0:
                handicap_str = f'\n⏱️ {ctx.author.mention} gets {int(challenger_hc)}s time handicap!'
            elif challengee_hc > 0:
                handicap_str = f'\n⏱️ {opponent.mention} gets {int(challengee_hc)}s time handicap!'
        else:
            handicap_str = '\n🚫 No handicap mode'
        
        await ctx.send(
            f'{ctx.author.mention} ({challenger_r}) is challenging'
            f' {opponent.mention} ({challengee_r}) to {ostr} {rstr}duel!{handicap_str}'
        )
        await asyncio.sleep(_DUEL_EXPIRY_TIME)
        if cf_common.user_db.cancel_duel(duelid, Duel.EXPIRED):
            message = (
                f'{ctx.author.mention}, your request to duel'
                f' {opponent.mention} has expired!'
            )
            embed = discord_common.embed_alert(message)
            await ctx.send(embed=embed)

    @duel.command(brief='Decline a duel')
    async def decline(self, ctx):
        active = cf_common.user_db.check_duel_decline(ctx.author.id)
        if not active:
            raise DuelCogError(f'{ctx.author.mention}, you are not being challenged!')

        duelid, challenger = active
        challenger = ctx.guild.get_member(challenger)
        cf_common.user_db.cancel_duel(duelid, Duel.DECLINED)
        message = (
            f'`{ctx.author.mention}` declined a challenge by {challenger.mention}.'
        )
        embed = discord_common.embed_alert(message)
        await ctx.send(embed=embed)

    @duel.command(brief='Withdraw a challenge')
    async def withdraw(self, ctx):
        active = cf_common.user_db.check_duel_withdraw(ctx.author.id)
        if not active:
            raise DuelCogError(f'{ctx.author.mention}, you are not challenging anyone.')

        duelid, challengee = active
        challengee = ctx.guild.get_member(challengee)
        cf_common.user_db.cancel_duel(duelid, Duel.WITHDRAWN)
        message = (
            f'{ctx.author.mention} withdrew a challenge to `{challengee.mention}`.'
        )
        embed = discord_common.embed_alert(message)
        await ctx.send(embed=embed)

    @duel.command(brief='Accept a duel')
    async def accept(self, ctx):
        active = cf_common.user_db.check_duel_accept(ctx.author.id)
        if not active:
            raise DuelCogError(f'{ctx.author.mention}, you are not being challenged.')

        duelid, challenger_id, name = active
        challenger = ctx.guild.get_member(challenger_id)
        await ctx.send(
            f'Duel between {challenger.mention} and'
            f' {ctx.author.mention} starting in 15 seconds!'
        )
        await asyncio.sleep(15)

        start_time = datetime.datetime.now().timestamp()
        rc = cf_common.user_db.start_duel(duelid, start_time)
        if rc != 1:
            raise DuelCogError(
                f'Unable to start the duel between {challenger.mention}'
                f' and {ctx.author.mention}.'
            )

        problem = cf_common.cache2.problem_cache.problem_by_name[name]
        title = f'{problem.index}. {problem.name}'
        desc = cf_common.cache2.contest_cache.get_contest(problem.contestId).name
        embed = discord.Embed(title=title, url=problem.url, description=desc)
        embed.add_field(name='Rating', value=problem.rating)
        await ctx.send(
            f'Starting duel: {challenger.mention} vs {ctx.author.mention}', embed=embed
        )

    @duel.command(brief='Complete a duel')
    async def complete(self, ctx):
        """Complete an ongoing duel and determine the winner.
        
        Checks both players' Codeforces submissions.
        Winner is determined by fastest solve time.
        
        **With Handicap:**
        If handicap is enabled, the lower-rated player's time
        is reduced by 30s per 100 rating difference.
        
        **Rating Changes (Official duels only):**
        Uses ELO formula - bigger upset = bigger rating swing.
        """
        active = cf_common.user_db.check_duel_complete(ctx.author.id)
        if not active:
            raise DuelCogError(f'{ctx.author.mention}, you are not in a duel.')

        (
            duelid,
            challenger_id,
            challengee_id,
            start_time,
            problem_name,
            contest_id,
            index,
            dtype,
            nohandicap,
        ) = active

        UNSOLVED = 0
        TESTING = -1

        async def get_solve_time(userid):
            handle = cf_common.user_db.get_handle(userid, ctx.guild.id)
            subs = [
                sub
                for sub in await cf.user.status(handle=handle)
                if (sub.verdict == 'OK' or sub.verdict == 'TESTING')
                and sub.problem.contestId == contest_id
                and sub.problem.index == index
            ]

            if not subs:
                return UNSOLVED
            if 'TESTING' in [sub.verdict for sub in subs]:
                return TESTING
            return min(
                subs, key=lambda sub: sub.creationTimeSeconds
            ).creationTimeSeconds

        challenger_time = await get_solve_time(challenger_id)
        challengee_time = await get_solve_time(challengee_id)

        if challenger_time == TESTING or challengee_time == TESTING:
            await ctx.send(
                f'Wait a bit, {ctx.author.mention}. A submission is still being judged.'
            )
            return

        challenger = ctx.guild.get_member(challenger_id)
        challengee = ctx.guild.get_member(challengee_id)
        
        # Get duel ratings for handicap calculation and display
        challenger_r = cf_common.user_db.get_duel_rating(challenger_id)
        challengee_r = cf_common.user_db.get_duel_rating(challengee_id)
        
        # Calculate and apply handicap if enabled
        challenger_hc, challengee_hc = (0, 0)
        if not nohandicap:
            challenger_hc, challengee_hc = calculate_handicap(challenger_r, challengee_r)
        
        # Apply handicap to solve times (subtract handicap = makes time "better")
        challenger_adjusted = challenger_time - start_time - challenger_hc if challenger_time else None
        challengee_adjusted = challengee_time - start_time - challengee_hc if challengee_time else None

        if challenger_time and challengee_time:
            # Both solved - compare adjusted times
            if challenger_adjusted != challengee_adjusted:
                if challenger_adjusted < challengee_adjusted:
                    winner = challenger
                    loser = challengee
                    winner_time = challenger_time
                    loser_time = challengee_time
                    winner_adjusted = challenger_adjusted
                    loser_adjusted = challengee_adjusted
                    win_status = Winner.CHALLENGER
                else:
                    winner = challengee
                    loser = challenger
                    winner_time = challengee_time
                    loser_time = challenger_time
                    winner_adjusted = challengee_adjusted
                    loser_adjusted = challenger_adjusted
                    win_status = Winner.CHALLENGEE
                
                raw_diff = cf_common.pretty_time_format(
                    abs((challengee_time - start_time) - (challenger_time - start_time)), always_seconds=True
                )
                adj_diff = cf_common.pretty_time_format(
                    abs(loser_adjusted - winner_adjusted), always_seconds=True
                )
                
                embed = complete_duel(
                    duelid,
                    ctx.guild.id,
                    win_status,
                    winner,
                    loser,
                    min(challenger_time, challengee_time),
                    1,
                    dtype,
                )
                
                if not nohandicap and (challenger_hc > 0 or challengee_hc > 0):
                    handicap_note = f'\n⏱️ (With handicap: adjusted time diff was {adj_diff})'
                    await ctx.send(
                        f'Both {challenger.mention} and {challengee.mention}'
                        f' solved it but {winner.mention} was {raw_diff} faster!{handicap_note}',
                        embed=embed,
                    )
                else:
                    await ctx.send(
                        f'Both {challenger.mention} and {challengee.mention}'
                        f' solved it but {winner.mention} was {raw_diff} faster!',
                        embed=embed,
                    )
            else:
                embed = complete_duel(
                    duelid,
                    ctx.guild.id,
                    Winner.DRAW,
                    challenger,
                    challengee,
                    challenger_time,
                    0.5,
                    dtype,
                )
                await ctx.send(
                    f'{challenger.mention} and {challengee.mention} solved the problem'
                    " in the exact same (adjusted) time! It's a draw!",
                    embed=embed,
                )

        elif challenger_time:
            embed = complete_duel(
                duelid,
                ctx.guild.id,
                Winner.CHALLENGER,
                challenger,
                challengee,
                challenger_time,
                1,
                dtype,
            )
            await ctx.send(
                f'{challenger.mention} beat {challengee.mention} in a duel!',
                embed=embed,
            )
        elif challengee_time:
            embed = complete_duel(
                duelid,
                ctx.guild.id,
                Winner.CHALLENGEE,
                challengee,
                challenger,
                challengee_time,
                1,
                dtype,
            )
            await ctx.send(
                f'{challengee.mention} beat {challenger.mention} in a duel!',
                embed=embed,
            )
        else:
            await ctx.send('Nobody solved the problem yet.')

    @duel.command(brief='Offer/Accept a draw')
    async def draw(self, ctx):
        active = cf_common.user_db.check_duel_draw(ctx.author.id)
        if not active:
            raise DuelCogError(f'{ctx.author.mention}, you are not in a duel.')

        duelid, challenger_id, challengee_id, start_time, dtype = active
        now = datetime.datetime.now().timestamp()
        if now - start_time < _DUEL_NO_DRAW_TIME:
            draw_time = cf_common.pretty_time_format(
                start_time + _DUEL_NO_DRAW_TIME - now
            )
            await ctx.send(
                f'Think more {ctx.author.mention}. You can offer a draw in {draw_time}.'
            )
            return

        if duelid not in self.draw_offers:
            self.draw_offers[duelid] = ctx.author.id
            offeree_id = (
                challenger_id if ctx.author.id != challenger_id else challengee_id
            )
            offeree = ctx.guild.get_member(offeree_id)
            await ctx.send(
                f'{ctx.author.mention} is offering a draw to {offeree.mention}!'
            )
            return

        if self.draw_offers[duelid] == ctx.author.id:
            await ctx.send(f"{ctx.author.mention}, you've already offered a draw.")
            return

        offerer = ctx.guild.get_member(self.draw_offers[duelid])
        embed = complete_duel(
            duelid, ctx.guild.id, Winner.DRAW, offerer, ctx.author, now, 0.5, dtype
        )
        await ctx.send(
            f'{ctx.author.mention} accepted draw offer by {offerer.mention}.',
            embed=embed,
        )

    @duel.command(brief='Show duelist profile')
    async def profile(self, ctx, member: discord.Member = None):
        member = member or ctx.author
        if not cf_common.user_db.is_duelist(member.id):
            raise DuelCogError(f'{member.mention} is not a registered duelist.')

        user = get_cf_user(member.id, ctx.guild.id)
        rating = cf_common.user_db.get_duel_rating(member.id)
        desc = (
            f'Duelist profile of {rating2rank(rating).title} {member.mention}'
            f' aka **[{user.handle}]({user.url})**'
        )
        embed = discord.Embed(description=desc, color=rating2rank(rating).color_embed)
        embed.add_field(name='Rating', value=rating, inline=True)

        wins = cf_common.user_db.get_duel_wins(member.id)
        num_wins = len(wins)
        embed.add_field(name='Wins', value=num_wins, inline=True)
        num_losses = cf_common.user_db.get_num_duel_losses(member.id)
        embed.add_field(name='Losses', value=num_losses, inline=True)
        num_draws = cf_common.user_db.get_num_duel_draws(member.id)
        embed.add_field(name='Draws', value=num_draws, inline=True)
        num_declined = cf_common.user_db.get_num_duel_declined(member.id)
        embed.add_field(name='Declined', value=num_declined, inline=True)
        num_rdeclined = cf_common.user_db.get_num_duel_rdeclined(member.id)
        embed.add_field(name='Got declined', value=num_rdeclined, inline=True)

        def duel_to_string(duel):
            start_time, finish_time, problem_name, challenger, challengee = duel
            duel_time = cf_common.pretty_time_format(
                finish_time - start_time, shorten=True, always_seconds=True
            )
            when = cf_common.days_ago(start_time)
            loser_id = challenger if member.id != challenger else challengee
            loser = get_cf_user(loser_id, ctx.guild.id)
            problem = cf_common.cache2.problem_cache.problem_by_name[problem_name]
            return (
                f'**[{problem.name}]({problem.url})** [{problem.rating}]'
                f' versus [{loser.handle}]({loser.url}) {when} in {duel_time}'
            )

        if wins:
            # sort by finish_time - start_time
            wins.sort(key=lambda duel: duel[1] - duel[0])
            embed.add_field(
                name='Fastest win', value=duel_to_string(wins[0]), inline=False
            )
            embed.add_field(
                name='Slowest win', value=duel_to_string(wins[-1]), inline=False
            )

        embed.set_thumbnail(url=f'{user.titlePhoto}')
        await ctx.send(embed=embed)

    def _paginate_duels(self, data, message, guild_id, show_id):
        def make_line(entry):
            duelid, start_time, finish_time, name, challenger, challengee, winner = (
                entry
            )
            duel_time = cf_common.pretty_time_format(
                finish_time - start_time, shorten=True, always_seconds=True
            )
            problem = cf_common.cache2.problem_cache.problem_by_name[name]
            when = cf_common.days_ago(start_time)
            idstr = f'{duelid}: '
            if winner != Winner.DRAW:
                loser = get_cf_user(
                    challenger if winner == Winner.CHALLENGEE else challengee, guild_id
                )
                winner = get_cf_user(
                    challenger if winner == Winner.CHALLENGER else challengee, guild_id
                )
                return (
                    f'{idstr if show_id else str()}[{name}]({problem.url})'
                    f' [{problem.rating}] won by [{winner.handle}]({winner.url})'
                    f' vs [{loser.handle}]({loser.url}) {when} in {duel_time}'
                )
            else:
                challenger = get_cf_user(challenger, guild_id)
                challengee = get_cf_user(challengee, guild_id)
                return (
                    f'{idstr if show_id else str()}[{name}]({problem.url})'
                    f' [{problem.rating}] drawn by'
                    f' [{challenger.handle}]({challenger.url}) and'
                    f' [{challengee.handle}]({challengee.url}) {when} after {duel_time}'
                )

        def make_page(chunk):
            log_str = '\n'.join(make_line(entry) for entry in chunk)
            embed = discord_common.cf_color_embed(description=log_str)
            return message, embed

        if not data:
            raise DuelCogError('There are no duels to show.')

        return [make_page(chunk) for chunk in paginator.chunkify(data, 7)]

    @duel.command(brief='Print head to head dueling history', aliases=['versushistory'])
    async def vshistory(
        self, ctx, member1: discord.Member = None, member2: discord.Member = None
    ):
        if not member1:
            raise DuelCogError('You need to specify one or two discord members.')

        member2 = member2 or ctx.author
        data = cf_common.user_db.get_pair_duels(member1.id, member2.id)
        wins, losses, draws = 0, 0, 0
        for _, _, _, _, challenger, challengee, winner in data:
            if winner != Winner.DRAW:
                winnerid = challenger if winner == Winner.CHALLENGER else challengee
                if winnerid == member1.id:
                    wins += 1
                else:
                    losses += 1
            else:
                draws += 1
        message = discord.utils.escape_mentions(
            f'`{member1.display_name}` ({wins}/{draws}/{losses})'
            f' `{member2.display_name}`'
        )
        pages = self._paginate_duels(data, message, ctx.guild.id, False)
        paginator.paginate(
            self.bot, ctx.channel, pages, wait_time=5 * 60, set_pagenum_footers=True
        )

    @duel.command(brief='Print user dueling history')
    async def history(self, ctx, member: discord.Member = None):
        member = member or ctx.author
        data = cf_common.user_db.get_duels(member.id)
        message = discord.utils.escape_mentions(
            f'dueling history of `{member.display_name}`'
        )
        pages = self._paginate_duels(data, message, ctx.guild.id, False)
        paginator.paginate(
            self.bot, ctx.channel, pages, wait_time=5 * 60, set_pagenum_footers=True
        )

    @duel.command(brief='Print recent duels')
    async def recent(self, ctx):
        data = cf_common.user_db.get_recent_duels()
        pages = self._paginate_duels(data, 'list of recent duels', ctx.guild.id, True)
        paginator.paginate(
            self.bot, ctx.channel, pages, wait_time=5 * 60, set_pagenum_footers=True
        )

    @duel.command(brief='Print list of ongoing duels')
    async def ongoing(self, ctx, member: discord.Member = None):
        def make_line(entry):
            start_time, name, challenger, challengee = entry
            problem = cf_common.cache2.problem_cache.problem_by_name[name]
            now = datetime.datetime.now().timestamp()
            when = cf_common.pretty_time_format(
                now - start_time, shorten=True, always_seconds=True
            )
            challenger = get_cf_user(challenger, ctx.guild.id)
            challengee = get_cf_user(challengee, ctx.guild.id)
            return (
                f'[{challenger.handle}]({challenger.url})'
                f' vs [{challengee.handle}]({challengee.url}):'
                f' [{name}]({problem.url}) [{problem.rating}] {when}'
            )

        def make_page(chunk):
            message = 'List of ongoing duels:'
            log_str = '\n'.join(make_line(entry) for entry in chunk)
            embed = discord_common.cf_color_embed(description=log_str)
            return message, embed

        member = member or ctx.author
        data = cf_common.user_db.get_ongoing_duels()
        if not data:
            raise DuelCogError('There are no ongoing duels.')

        pages = [make_page(chunk) for chunk in paginator.chunkify(data, 7)]
        paginator.paginate(
            self.bot, ctx.channel, pages, wait_time=5 * 60, set_pagenum_footers=True
        )

    @duel.command(brief='Show duelists')
    async def ranklist(self, ctx):
        """Show the list of duelists with their duel rating."""
        users = [
            (ctx.guild.get_member(user_id), rating)
            for user_id, rating in cf_common.user_db.get_duelists()
        ]
        users = [
            (member, cf_common.user_db.get_handle(member.id, ctx.guild.id), rating)
            for member, rating in users
            if member is not None
            and cf_common.user_db.get_num_duel_completed(member.id) > 0
        ]

        _PER_PAGE = 10

        def make_page(chunk, page_num):
            style = table.Style('{:>}  {:<}  {:<}  {:<}')
            t = table.Table(style)
            t += table.Header('#', 'Name', 'Handle', 'Rating')
            t += table.Line()
            for index, (member, handle, rating) in enumerate(chunk):
                rating_str = f'{rating} ({rating2rank(rating).title_abbr})'
                t += table.Data(
                    _PER_PAGE * page_num + index,
                    f'{member.display_name}',
                    handle,
                    rating_str,
                )

            table_str = f'```\n{t}\n```'
            embed = discord_common.cf_color_embed(description=table_str)
            return 'List of duelists', embed

        if not users:
            raise DuelCogError('There are no active duelists.')

        pages = [
            make_page(chunk, k)
            for k, chunk in enumerate(paginator.chunkify(users, _PER_PAGE))
        ]
        paginator.paginate(
            self.bot, ctx.channel, pages, wait_time=5 * 60, set_pagenum_footers=True
        )

    async def invalidate_duel(self, ctx, duelid, challenger_id, challengee_id):
        rc = cf_common.user_db.invalidate_duel(duelid)
        if rc == 0:
            raise DuelCogError(f'Unable to invalidate duel {duelid}.')

        challenger = ctx.guild.get_member(challenger_id)
        challengee = ctx.guild.get_member(challengee_id)
        await ctx.send(
            f'Duel between {challenger.mention} and'
            f' {challengee.mention} has been invalidated.'
        )

    @duel.command(brief='Invalidate the duel')
    async def invalidate(self, ctx):
        """Declare your duel invalid. Use this if you've solved the problem
        prior to the duel. You can only use this functionality during the first
        60 seconds of the duel."""
        active = cf_common.user_db.check_duel_complete(ctx.author.id)
        if not active:
            raise DuelCogError(f'{ctx.author.mention}, you are not in a duel.')

        duelid, challenger_id, challengee_id, start_time, _, _, _, _ = active
        if datetime.datetime.now().timestamp() - start_time > _DUEL_INVALIDATE_TIME:
            raise DuelCogError(
                f'{ctx.author.mention}, you can no longer invalidate your duel.'
            )
        await self.invalidate_duel(ctx, duelid, challenger_id, challengee_id)

    @duel.command(brief='Invalidate a duel', usage='[duelist]')
    @commands.has_any_role(constants.TLE_ADMIN, constants.TLE_MODERATOR)
    async def _invalidate(self, ctx, member: discord.Member):
        """Declare an ongoing duel invalid."""
        active = cf_common.user_db.check_duel_complete(member.id)
        if not active:
            raise DuelCogError(f'{member.mention} is not in a duel.')

        duelid, challenger_id, challengee_id, _, _, _, _, _ = active
        await self.invalidate_duel(ctx, duelid, challenger_id, challengee_id)

    @duel.command(brief='Plot rating', usage='[duelist]')
    async def rating(self, ctx, *members: discord.Member):
        """Plot duelist's rating."""
        members = members or (ctx.author,)
        if len(members) > 5:
            raise DuelCogError('Cannot plot more than 5 duelists at once.')

        duelists = [member.id for member in members]
        duels = cf_common.user_db.get_complete_official_duels()
        rating = dict()
        plot_data = defaultdict(list)
        time_tick = 0
        for challenger, challengee, winner, _finish_time in duels:
            challenger_r = rating.get(challenger, 1500)
            challengee_r = rating.get(challengee, 1500)
            if winner == Winner.CHALLENGER:
                delta = round(elo_delta(challenger_r, challengee_r, 1))
            elif winner == Winner.CHALLENGEE:
                delta = round(elo_delta(challenger_r, challengee_r, 0))
            else:
                delta = round(elo_delta(challenger_r, challengee_r, 0.5))

            rating[challenger] = challenger_r + delta
            rating[challengee] = challengee_r - delta
            if challenger in duelists or challengee in duelists:
                if challenger in duelists:
                    plot_data[challenger].append((time_tick, rating[challenger]))
                if challengee in duelists:
                    plot_data[challengee].append((time_tick, rating[challengee]))
                time_tick += 1

        if time_tick == 0:
            raise DuelCogError('Nothing to plot.')

        plt.clf()
        # plot at least from mid gray to mid purple
        min_rating = 1350
        max_rating = 1550
        for rating_data in plot_data.values():
            for _tick, rating in rating_data:
                min_rating = min(min_rating, rating)
                max_rating = max(max_rating, rating)

            x, y = zip(*rating_data, strict=False)
            plt.plot(
                x,
                y,
                linestyle='-',
                marker='o',
                markersize=2,
                markerfacecolor='white',
                markeredgewidth=0.5,
            )

        gc.plot_rating_bg(DUEL_RANKS)
        plt.xlim(0, time_tick - 1)
        plt.ylim(min_rating - 100, max_rating + 100)

        labels = [
            gc.StrWrap(
                '{} ({})'.format(
                    ctx.guild.get_member(duelist).display_name, rating_data[-1][1]
                )
            )
            for duelist, rating_data in plot_data.items()
        ]
        plt.legend(labels, loc='upper left', prop=gc.fontprop)

        discord_file = gc.get_current_figure_as_file()
        embed = discord_common.cf_color_embed(title='Duel rating graph')
        discord_common.attach_image(embed, discord_file)
        discord_common.set_author_footer(embed, ctx.author)
        await ctx.send(embed=embed, file=discord_file)

    # Multi-player duel commands
    
    @duel.command(brief='Start a multi-player duel', usage='@user1 @user2 ... [num_problems] [rating...] [+tag..] [~tag..] [nohandicap]')
    async def multistart(self, ctx, *args):
        """Start a multi-player duel with 2-10 participants.
        
        **Arguments:**
        • @users - Mention 1-9 other users (you're auto-included)
        • num_problems - Number of problems (1-5, default: 3)
        • rating - Single rating OR multiple ratings for each problem
        • +tag - Include problems with this tag
        • ~tag - Exclude problems with this tag
        • nohandicap - Disable time handicap
        
        **Scoring:**
        Ranked by problems solved, ties broken by total time.
        
        **Rating Changes (Official duels):**
        • 1st: +40, 2nd: +20, 3rd: +10
        • 4th+ (solved >0): -5
        • 0 solved: -15
        
        **Examples:**
        `;duel multistart @alice @bob` - 2 players, 3 problems
        `;duel multistart @a @b @c 5 1600` - 3 players, 5 problems at 1600
        `;duel multistart @user 1300 1500 1700` - 3 problems at different ratings
        `;duel multistart @user 3 nohandicap` - No handicap mode
        """
        # Parse mentions and arguments
        mentions = []
        remaining_args = []
        
        for arg in args:
            # Try to parse as mention
            if arg.startswith('<@') and arg.endswith('>'):
                user_id_str = arg[2:-1].replace('!', '')
                try:
                    user_id = int(user_id_str)
                    member = ctx.guild.get_member(user_id)
                    if member:
                        mentions.append(member)
                    continue
                except ValueError:
                    pass
            remaining_args.append(arg)
        
        # Check for nohandicap option
        nohandicap = False
        for arg in remaining_args[:]:
            if arg.lower() == 'nohandicap':
                nohandicap = True
                remaining_args.remove(arg)
        
        # Add creator to participants
        all_participants = [ctx.author] + mentions
        
        if len(all_participants) < 2:
            raise DuelCogError('You need at least 2 participants (including yourself) for a multi-player duel!')
        
        if len(all_participants) > 10:
            raise DuelCogError('Maximum 10 participants allowed for a multi-player duel!')
        
        # Check all participants are duelists
        for participant in all_participants:
            if not cf_common.user_db.is_duelist(participant.id):
                raise DuelCogError(f'{participant.mention} is not a registered duelist!')
        
        # Check none are in active duels
        for participant in all_participants:
            if cf_common.user_db.check_duel_challenge(participant.id):
                raise DuelCogError(f'{participant.mention} is currently in a 1v1 duel!')
            if cf_common.user_db.check_multiplayer_duel_participant(participant.id):
                raise DuelCogError(f'{participant.mention} is currently in a multi-player duel!')
        
        # Parse ratings - collect all numbers that look like CF ratings (800-3500)
        rating_args = []
        num_problems_arg = None
        for arg in remaining_args[:]:
            try:
                n = int(arg)
                if 800 <= n <= 3500 and n % 100 == 0:
                    # Looks like a rating
                    rating_args.append(n)
                    remaining_args.remove(arg)
                elif 1 <= n <= 5:
                    # Looks like num_problems
                    num_problems_arg = n
                    remaining_args.remove(arg)
            except ValueError:
                pass
        
        # Parse tags
        tags = cf_common.parse_tags(remaining_args, prefix='+')
        bantags = cf_common.parse_tags(remaining_args, prefix='~')
        
        # Get handles and calculate suggested rating
        userids = [p.id for p in all_participants]
        handles = [cf_common.user_db.get_handle(uid, ctx.guild.id) for uid in userids]
        
        # Resolve handles
        await cf_common.resolve_handles(ctx, self.converter, 
            tuple('!' + str(p) for p in all_participants))
        
        users = [cf_common.user_db.fetch_cf_user(handle) for handle in handles]
        lowest_rating = min(user.rating or 0 for user in users)
        min_cf_rating = round(lowest_rating, -2)  # Min CF rating rounded to nearest 100
        suggested_rating = max(min_cf_rating + _DUEL_RATING_DELTA, 500)
        
        # Determine ratings mode
        # Multi-player duels are always official (no custom URL option)
        if len(rating_args) >= 2:
            # Multiple ratings mode - each rating is for one problem
            target_ratings = sorted(rating_args)  # Sort ascending
            num_problems = len(target_ratings)
            if num_problems > 5:
                raise DuelCogError('Maximum 5 problems allowed!')
            rating_display = ' → '.join(str(r) for r in target_ratings)
            avg_rating = sum(target_ratings) // len(target_ratings)
        elif len(rating_args) == 1:
            # Single rating mode
            single_rating = rating_args[0]
            num_problems = num_problems_arg if num_problems_arg else 3
            target_ratings = [single_rating] * num_problems
            rating_display = str(single_rating)
            avg_rating = single_rating
        else:
            # Default rating mode
            num_problems = num_problems_arg if num_problems_arg else 3
            target_ratings = [suggested_rating] * num_problems
            rating_display = str(suggested_rating)
            avg_rating = suggested_rating
        
        # Multi-player duels are always official
        dtype = DuelType.OFFICIAL
        
        # Get submissions to filter solved problems
        submissions = [await cf.user.status(handle=handle) for handle in handles]
        solved = {
            sub.problem.name
            for subs in submissions
            for sub in subs
            if sub.verdict != 'COMPILATION_ERROR'
        }
        
        # Get previously seen problems in duels
        seen = {
            name
            for userid in userids
            for (name,) in cf_common.user_db.get_duel_problem_names(userid)
        }
        
        # Get available problems for a rating
        def get_problems(r):
            return [
                prob
                for prob in cf_common.cache2.problem_cache.problems
                if prob.rating == r
                and prob.name not in solved
                and prob.name not in seen
                and not any(cf_common.is_contest_writer(prob.contestId, h) for h in handles)
                and not cf_common.is_nonstandard_problem(prob)
                and prob.matches_all_tags(tags)
                and not prob.matches_any_tag(bantags)
            ]
        
        # Select one problem per target rating
        selected_problems = []
        used_names = set()
        for target_r in target_ratings:
            # Find problems at this rating, or fall back to lower ratings
            problem_found = False
            for r in range(target_r, 400, -100):
                problems = [p for p in get_problems(r) if p.name not in used_names]
                if problems:
                    problems.sort(key=lambda p: cf_common.cache2.contest_cache.get_contest(p.contestId).startTimeSeconds)
                    choice = max(random.randrange(len(problems)) for _ in range(2))
                    selected = problems[choice]
                    selected_problems.append(selected)
                    used_names.add(selected.name)
                    problem_found = True
                    break
            if not problem_found:
                raise DuelCogError(f'No unsolved problems found at rating {target_r}!')
        
        # Sort selected problems by rating (ascending)
        selected_problems.sort(key=lambda p: p.rating or 0)
        
        # Create the duel
        issue_time = datetime.datetime.now().timestamp()
        duel_id = cf_common.user_db.create_multiplayer_duel(
            ctx.author.id, ctx.guild.id, issue_time, num_problems, avg_rating, dtype, nohandicap
        )

        
        # Add all participants
        for participant in all_participants:
            status = ParticipantStatus.ACCEPTED if participant == ctx.author else ParticipantStatus.INVITED
            cf_common.user_db.add_multiplayer_participant(duel_id, participant.id, status)
        
        # Add all problems
        for i, problem in enumerate(selected_problems):
            cf_common.user_db.add_multiplayer_problem(duel_id, problem, i + 1)
        
        # Build participant list with ratings
        participant_list = []
        for p in all_participants:
            p_rating = cf_common.user_db.get_duel_rating(p.id)
            participant_list.append(f'{p.mention} ({p_rating})')
        participant_mentions = ', '.join(participant_list)
        
        ostr = 'official'  # Multi-player duels are always official
        handicap_mode = '🚫 No handicap' if nohandicap else '⏱️ Handicap enabled'
        
        embed = discord_common.cf_color_embed(title='🎯 Multi-Player Duel Created!')
        embed.add_field(name='Creator', value=ctx.author.mention, inline=False)
        embed.add_field(name='Participants', value=participant_mentions, inline=False)
        embed.add_field(name='Problems', value=str(num_problems), inline=True)
        embed.add_field(name='Rating', value=rating_display, inline=True)
        embed.add_field(name='Type', value=ostr.capitalize(), inline=True)
        embed.add_field(name='Mode', value=handicap_mode, inline=True)
        embed.add_field(
            name='Status', 
            value=f'Waiting for all participants to accept. Use `;duel multiaccept` to join!',
            inline=False
        )
        
        await ctx.send(embed=embed)
        
        # Start expiry timer
        await asyncio.sleep(_DUEL_EXPIRY_TIME)
        if cf_common.user_db.cancel_multiplayer_duel(duel_id, Duel.EXPIRED):
            message = f'{ctx.author.mention}, your multi-player duel has expired!'
            embed = discord_common.embed_alert(message)
            await ctx.send(embed=embed)
    
    @duel.command(brief='Accept a multi-player duel invitation')
    async def multiaccept(self, ctx):
        """Accept your pending multi-player duel invitation.
        
        Once all participants accept, the duel starts after a 15-second countdown.
        Problems will be revealed to all participants at the same time.
        """
        duel_info = cf_common.user_db.get_multiplayer_duel_by_user(ctx.author.id)
        if not duel_info:
            raise DuelCogError(f'{ctx.author.mention}, you are not invited to any multi-player duel!')
        
        # duel_info is a tuple: (id, creator_id, guild_id, issue_time, start_time, ...)
        duel_id = duel_info[0]
        
        # Check if already accepted
        # participants are tuples: (user_id, status, problems_solved, total_time, placement, rating_delta)
        participants = cf_common.user_db.get_multiplayer_participants(duel_id)
        my_participant = next((p for p in participants if p[0] == ctx.author.id), None)
        
        if my_participant and my_participant[1] == ParticipantStatus.ACCEPTED:
            raise DuelCogError(f'{ctx.author.mention}, you have already accepted this duel!')
        
        # Accept the duel
        cf_common.user_db.update_participant_status(duel_id, ctx.author.id, ParticipantStatus.ACCEPTED)
        
        await ctx.send(f'{ctx.author.mention} has accepted the multi-player duel!')
        
        # Check if all participants have accepted
        if cf_common.user_db.check_all_accepted(duel_id):
            await ctx.send('All participants have accepted! Starting duel in 15 seconds...')
            await asyncio.sleep(15)
            
            start_time = datetime.datetime.now().timestamp()
            rc = cf_common.user_db.start_multiplayer_duel(duel_id, start_time)
            
            if rc != 1:
                raise DuelCogError('Unable to start the multi-player duel.')
            
            # Get and display problems
            problems = cf_common.user_db.get_multiplayer_problems(duel_id)
            participants = cf_common.user_db.get_multiplayer_participants(duel_id)
            participant_mentions = ', '.join(
                ctx.guild.get_member(p[0]).mention 
                for p in participants 
                if p[1] == ParticipantStatus.ACCEPTED
            )
            
            embed = discord_common.cf_color_embed(title='⚔️ Multi-Player Duel Started!')
            embed.add_field(name='Participants', value=participant_mentions, inline=False)
            
            # prob_info tuple: (problem_name, contest_id, p_index, problem_order)
            problem_list = []
            for prob_info in problems:
                problem = cf_common.cache2.problem_cache.problem_by_name[prob_info[0]]
                problem_list.append(
                    f'{prob_info[3]}. [{problem.name}]({problem.url}) [{problem.rating}]'
                )
            
            embed.add_field(name='Problems', value='\n'.join(problem_list), inline=False)
            embed.add_field(name='Good luck!', value='🍀', inline=False)
            
            await ctx.send(embed=embed)
    
    @duel.command(brief='Decline a multi-player duel invitation')
    async def multidecline(self, ctx):
        """Decline your pending multi-player duel invitation.
        
        If too few participants remain (<2), the duel is cancelled.
        You can only decline pending duels, not ongoing ones.
        """
        duel_info = cf_common.user_db.get_multiplayer_duel_by_user(ctx.author.id)
        if not duel_info:
            raise DuelCogError(f'{ctx.author.mention}, you are not invited to any multi-player duel!')
        
        # duel_info tuple: (id, creator_id, guild_id, issue_time, start_time, finish_time, status, type, ...)
        duel_id = duel_info[0]
        duel_status = duel_info[6]
        
        if duel_status != Duel.PENDING:
            raise DuelCogError(f'{ctx.author.mention}, this duel has already started!')
        
        # Decline the duel
        cf_common.user_db.update_participant_status(duel_id, ctx.author.id, ParticipantStatus.DECLINED)
        
        # Check remaining participants (status is at index 1)
        participants = cf_common.user_db.get_multiplayer_participants(duel_id)
        active_count = sum(1 for p in participants if p[1] != ParticipantStatus.DECLINED)
        
        if active_count < 2:
            # Cancel the duel if too few participants
            cf_common.user_db.cancel_multiplayer_duel(duel_id, Duel.DECLINED)
            await ctx.send(f'{ctx.author.mention} declined. Not enough participants remain. Duel cancelled.')
        else:
            await ctx.send(f'{ctx.author.mention} has declined the multi-player duel.')
    
    @duel.command(brief='Cancel your pending multi-player duel')
    async def multicancel(self, ctx):
        """Cancel a pending multi-player duel you created.
        
        Only the creator can cancel. Only works for pending duels (not started).
        """
        duel_info = cf_common.user_db.get_multiplayer_duel_by_user(ctx.author.id)
        if not duel_info:
            raise DuelCogError(f'{ctx.author.mention}, you are not in any multi-player duel!')
        
        # duel_info tuple: (id, creator_id, guild_id, issue_time, start_time, finish_time, status, ...)
        duel_id = duel_info[0]
        creator_id = duel_info[1]
        duel_status = duel_info[6]
        
        if duel_status != Duel.PENDING:
            raise DuelCogError(f'{ctx.author.mention}, this duel has already started! Use `;duel multicomplete` when done.')
        
        if creator_id != ctx.author.id:
            raise DuelCogError(f'{ctx.author.mention}, only the creator can cancel the duel. Use `;duel multidecline` to leave.')
        
        # Cancel the duel
        cf_common.user_db.cancel_multiplayer_duel(duel_id, Duel.WITHDRAWN)
        await ctx.send(f'{ctx.author.mention} has cancelled the multi-player duel.')
    
    @duel.command(brief='Withdraw from an ongoing multi-player duel')
    async def multiwithdraw(self, ctx):
        """Withdraw from an ongoing multi-player duel.
        
        You can withdraw from a duel that has started but not completed.
        If too few participants remain (<2), the duel is cancelled.
        """
        duel_info = cf_common.user_db.get_multiplayer_duel_by_user(ctx.author.id)
        if not duel_info:
            raise DuelCogError(f'{ctx.author.mention}, you are not in any multi-player duel!')
        
        # duel_info tuple: (id, creator_id, guild_id, issue_time, start_time, finish_time, status, ...)
        duel_id = duel_info[0]
        duel_status = duel_info[6]
        
        if duel_status == Duel.PENDING:
            raise DuelCogError(f'{ctx.author.mention}, use `;duel multidecline` or `;duel multicancel` for pending duels.')
        
        if duel_status != Duel.ONGOING:
            raise DuelCogError(f'{ctx.author.mention}, this duel is no longer active.')
        
        # Mark participant as declined/withdrawn
        cf_common.user_db.update_participant_status(duel_id, ctx.author.id, ParticipantStatus.DECLINED)
        
        # Check remaining active participants
        participants = cf_common.user_db.get_multiplayer_participants(duel_id)
        active_count = sum(1 for p in participants if p[1] == ParticipantStatus.ACCEPTED)
        
        if active_count < 2:
            cf_common.user_db.cancel_multiplayer_duel(duel_id, Duel.WITHDRAWN)
            await ctx.send(f'{ctx.author.mention} withdrew. Not enough participants remain. Duel cancelled.')
        else:
            await ctx.send(f'{ctx.author.mention} has withdrawn from the multi-player duel.')
    
    @duel.command(brief='Complete a multi-player duel')
    async def multicomplete(self, ctx):
        """Complete an ongoing multi-player duel and show results.
        
        Checks all participants' Codeforces submissions.
        
        **Scoring:**
        Ranked by problems solved, ties broken by total time.
        
        **Rating Changes (Official duels):**
        • 1st: +40, 2nd: +20, 3rd: +10
        • 4th+ with solves: -5
        • 0 solved: -15
        """
        duel_info = cf_common.user_db.get_multiplayer_duel_by_user(ctx.author.id)
        if not duel_info:
            raise DuelCogError(f'{ctx.author.mention}, you are not in a multi-player duel!')
        
        # duel_info tuple: (id, creator_id, guild_id, issue_time, start_time, finish_time, status, type, num_problems, rating, nohandicap)
        duel_id = duel_info[0]
        duel_start_time = duel_info[4]
        duel_status = duel_info[6]
        duel_type = duel_info[7]
        duel_num_problems = duel_info[8]
        
        if duel_status != Duel.ONGOING:
            raise DuelCogError(f'{ctx.author.mention}, this duel has not started yet!')
        
        participants = cf_common.user_db.get_multiplayer_participants(duel_id)
        problems = cf_common.user_db.get_multiplayer_problems(duel_id)
        
        UNSOLVED = 0
        TESTING = -1
        
        # Check each participant's progress
        # participant tuple: (user_id, status, problems_solved, total_time, placement, rating_delta)
        participant_results = []
        for participant in participants:
            if participant[1] != ParticipantStatus.ACCEPTED:
                continue
            
            user_id = participant[0]
            handle = cf_common.user_db.get_handle(user_id, ctx.guild.id)
            subs = await cf.user.status(handle=handle)
            
            solved_count = 0
            total_time = 0.0
            has_testing = False
            
            # prob_info tuple: (problem_name, contest_id, p_index, problem_order)
            for prob_info in problems:
                problem_subs = [
                    sub for sub in subs
                    if sub.problem.contestId == prob_info[1]
                    and sub.problem.index == prob_info[2]
                    and sub.creationTimeSeconds >= duel_start_time
                ]
                
                if not problem_subs:
                    continue
                
                if any(sub.verdict == 'TESTING' for sub in problem_subs):
                    has_testing = True
                    break
                
                accepted_subs = [sub for sub in problem_subs if sub.verdict == 'OK']
                if accepted_subs:
                    solved_count += 1
                    solve_time = min(sub.creationTimeSeconds for sub in accepted_subs)
                    total_time += solve_time - duel_start_time
            
            if has_testing:
                await ctx.send(
                    f'Wait a bit, {ctx.author.mention}. A submission is still being judged.'
                )
                return
            
            participant_results.append({
                'user_id': user_id,
                'solved': solved_count,
                'time': total_time,
                'member': ctx.guild.get_member(user_id)
            })
            
            # Update progress in database
            cf_common.user_db.update_participant_progress(duel_id, user_id, solved_count, total_time)
        
        # Sort by problems solved (desc), then by time (asc)
        participant_results.sort(key=lambda x: (-x['solved'], x['time']))
        
        # Calculate placements and rating deltas
        placements_with_deltas = []
        for i, result in enumerate(participant_results):
            placement = i + 1
            
            # Rating delta calculation (simple version)
            if duel_type == DuelType.OFFICIAL:
                if placement == 1:
                    delta = 40
                elif placement == 2:
                    delta = 20
                elif placement == 3:
                    delta = 10
                elif result['solved'] > 0:
                    delta = -5
                else:
                    delta = -15
            else:
                delta = 0
            
            placements_with_deltas.append((result['user_id'], placement, delta))
        
        # Complete the duel
        finish_time = datetime.datetime.now().timestamp()
        rc = cf_common.user_db.complete_multiplayer_duel(
            duel_id, finish_time, placements_with_deltas, duel_type
        )
        
        if rc == 0:
            raise DuelCogError('Unable to complete the multi-player duel.')
        
        # Display results
        embed = discord_common.cf_color_embed(title='🏆 Multi-Player Duel Complete!')
        
        medals = ['🥇', '🥈', '🥉']
        for i, result in enumerate(participant_results):
            medal = medals[i] if i < 3 else f'{i+1}.'
            member = result['member']
            solved = result['solved']
            time_str = cf_common.pretty_time_format(result['time'], always_seconds=True)
            delta = placements_with_deltas[i][2]
            delta_str = f' ({delta:+d})' if duel_type == DuelType.OFFICIAL else ''
            
            embed.add_field(
                name=f'{medal} {member.display_name}',
                value=f'{solved}/{duel_num_problems} solved in {time_str}{delta_str}',
                inline=False
            )
        
        if duel_type == DuelType.OFFICIAL:
            rating_changes = []
            for i, result in enumerate(participant_results):
                member = result['member']
                delta = placements_with_deltas[i][2]
                old_rating = cf_common.user_db.get_duel_rating(result['user_id']) - delta
                new_rating = old_rating + delta
                rating_changes.append(f'{member.mention}: {old_rating} → {new_rating} ({delta:+d})')
            
            embed.add_field(name='Rating Changes', value='\n'.join(rating_changes), inline=False)
        
        await ctx.send(embed=embed)
    
    @duel.command(brief='View multi-player duel history', usage='[@user]')
    async def multihistory(self, ctx, member: discord.Member = None):
        """View multi-player duel history for yourself or another user.
        
        Shows last 10 completed multi-player duels with:
        • Placement (with medal for top 3)
        • Problems solved / total
        • Total time taken
        • Rating change (for official duels)
        """
        member = member or ctx.author
        history = cf_common.user_db.get_multiplayer_duel_history(member.id)
        
        if not history:
            raise DuelCogError(f'{member.mention} has no completed multi-player duels.')
        
        embed = discord_common.cf_color_embed(
            title=f'Multi-Player Duel History - {member.display_name}'
        )
        
        # duel tuple: (id, start_time, finish_time, num_problems, placement, problems_solved, total_time, rating_delta)
        for i, duel in enumerate(history[:10]):  # Show last 10
            placement = duel[4]
            total_time = duel[6]
            rating_delta = duel[7]
            problems_solved = duel[5]
            num_problems = duel[3]
            start_time = duel[1]
            
            placement_emoji = ['🥇', '🥈', '🥉'][placement - 1] if placement <= 3 else f'{placement}.'
            time_str = cf_common.pretty_time_format(total_time, always_seconds=True)
            delta_str = f' ({rating_delta:+d})' if rating_delta else ''
            
            embed.add_field(
                name=f'{placement_emoji} {cf_common.days_ago(start_time)}',
                value=f'{problems_solved}/{num_problems} solved in {time_str}{delta_str}',
                inline=False
            )
        
        await ctx.send(embed=embed)

    @discord_common.send_error_if(DuelCogError, cf_common.ResolveHandleError)
    async def cog_command_error(self, ctx, error):
        pass


def setup(bot):
    bot.add_cog(Dueling(bot))
