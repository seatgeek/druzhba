import datetime
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class FakeArgs:
    log_level: Optional[str] = None
    database: Optional[str] = None
    tables: Optional[List[str]] = None
    num_processes: Optional[int] = None
    compile_only: Optional[bool] = None
    print_sql_only: Optional[bool] = None
    validate_only: Optional[bool] = None
    full_refresh: Optional[bool] = None
    rebuild: Optional[bool] = None


class TimeFixtures:
    t0 = datetime.datetime(2019, 1, 1, 0, 0, 0)
    t1 = t0 + datetime.timedelta(seconds=5)
    t2 = t0 + datetime.timedelta(seconds=10)
    t3 = t0 + datetime.timedelta(seconds=15)
    tmin = datetime.datetime.min
