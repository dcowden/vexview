import cachetools.func
import motherduck
import polars as pl

CACHE_SECONDS = 600

@cachetools.func.ttl_cache(maxsize=128, ttl=CACHE_SECONDS)
def get_matches() -> pl.DataFrame:
    return motherduck.get_matches()

@cachetools.func.ttl_cache(maxsize=128, ttl=CACHE_SECONDS)
def get_events() -> pl.DataFrame:
    return motherduck.get_events()

@cachetools.func.ttl_cache(maxsize=128, ttl=CACHE_SECONDS)
def get_teams() -> pl.DataFrame:
    return motherduck.get_teams()

@cachetools.func.ttl_cache(maxsize=128, ttl=CACHE_SECONDS)
def get_rankings() -> pl.DataFrame:
    return motherduck.get_rankings()