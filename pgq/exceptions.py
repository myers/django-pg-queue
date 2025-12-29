from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .models import BaseJob
else:
    BaseJob = None


class PgqException(Exception):
    """Base exception for pgq task processing errors."""

    job: Optional[BaseJob] = None

    def __init__(self, job: Optional[BaseJob] = None, message: str = ""):
        self.job = job
        super().__init__(message)
