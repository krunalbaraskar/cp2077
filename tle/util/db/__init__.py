import os
import logging

logger = logging.getLogger(__name__)

# Use Supabase if configured, otherwise fall back to SQLite
if os.environ.get("SUPABASE_URL") and os.environ.get("SUPABASE_KEY"):
    try:
        from .supabase_db_conn import SupabaseDbConn as UserDbConn
        from .supabase_db_conn import (
            Gitgud, Duel, Winner, DuelType, RatedVC, ParticipantStatus,
            UserDbError, DatabaseDisabledError, DummyUserDbConn, UniqueConstraintFailed
        )
        logger.info("Using Supabase database backend")
    except Exception as e:
        logger.warning(f"Failed to load Supabase backend: {e}, falling back to SQLite")
        from .user_db_conn import *
else:
    from .user_db_conn import *
    logger.info("Using SQLite database backend")

from .cache_db_conn import *
