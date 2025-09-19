"""
Timezone utilities for ETL pipeline.
Provides consistent timezone handling across all modules.
"""
from datetime import datetime, timezone, timedelta

# Define UTC+8 timezone (Asia/Taipei)
TAIPEI_TZ = timezone(timedelta(hours=8))

def now_taipei() -> datetime:
    """
    Get current datetime in UTC+8 (Asia/Taipei) timezone.
    Precision truncated to seconds.
    
    Returns:
        Current datetime with UTC+8 timezone (precision to seconds)
    """
    dt = datetime.now(TAIPEI_TZ)
    return dt.replace(microsecond=0)

def now_taipei_iso() -> str:
    """
    Get current datetime in UTC+8 timezone as ISO format string.
    Precision truncated to seconds.
    
    Returns:
        ISO format datetime string in UTC+8 (precision to seconds)
    """
    return now_taipei().isoformat()

def to_taipei_tz(dt: datetime) -> datetime:
    """
    Convert datetime to UTC+8 timezone.
    
    Args:
        dt: datetime object (aware or naive)
        
    Returns:
        datetime object in UTC+8 timezone
    """
    if dt.tzinfo is None:
        # Assume naive datetime is in UTC
        dt = dt.replace(tzinfo=timezone.utc)
    
    return dt.astimezone(TAIPEI_TZ)