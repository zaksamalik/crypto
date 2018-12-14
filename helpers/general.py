import re
from datetime import datetime


def get_utc_ts_str():
    """Get current UTC timestamp as string for appending to end of file names.

    Returns: current UTC timestamp as string with `:` and `.` replaced with `_`.

    """
    return re.sub('[:. ]', '_', datetime.utcnow().__str__())
