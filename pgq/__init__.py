"""
django-pg-queue - PostgreSQL-backed task queue for Django.

This package provides both a legacy API and Django 6.0 django.tasks
compatible API for background task processing using PostgreSQL.

Legacy API (existing):
    from pgq.queue import AtLeastOnceQueue, AtMostOnceQueue
    from pgq.decorators import task, repeat

Django 6.0 compatible API (new):
    from pgq.backend import task, PostgresQueueBackend, TaskResult
    from pgq.decorators import legacy_task, migrate_task
"""

# Version
__version__ = "0.9.0"

# Lazy imports to avoid Django app registry issues
# Users should import from submodules directly:
#   from pgq.models import Job
#   from pgq.queue import AtLeastOnceQueue
#   from pgq.backend import task, PostgresQueueBackend


def __getattr__(name):
    """Lazy import for top-level package attributes."""
    # Models
    if name in ("Job", "BaseJob", "TaskResultStatus", "DEFAULT_QUEUE_NAME"):
        from pgq import models
        return getattr(models, name)

    # Legacy Queue classes
    if name in ("AtLeastOnceQueue", "AtMostOnceQueue", "Queue", "BaseQueue"):
        from pgq import queue
        return getattr(queue, name)

    # Legacy decorators
    if name == "legacy_task_decorator":
        from pgq.decorators import task
        return task
    if name in ("repeat", "retry", "AsyncTask", "JobMeta", "legacy_task",
                "migrate_task", "LegacyJob", "LegacyQueue"):
        from pgq import decorators
        return getattr(decorators, name)

    # Exceptions
    if name in ("PgqException", "PgqIncorrectQueue", "PgqNoDefinedQueue"):
        from pgq import exceptions
        return getattr(exceptions, name)

    # Legacy worker
    if name == "Worker":
        from pgq import commands
        return commands.Worker

    # Django 6.0 django.tasks API
    if name in ("PostgresQueueBackend", "Task", "TaskResult", "TaskError",
                "ResultStatus", "InvalidTask", "TaskResultDoesNotExist",
                "task", "get_task_backend", "configure_backend",
                "get_default_backend"):
        from pgq import backend
        return getattr(backend, name)

    raise AttributeError(f"module 'pgq' has no attribute '{name}'")


__all__ = [
    # Version
    "__version__",
    # Models
    "Job",
    "BaseJob",
    "TaskResultStatus",
    "DEFAULT_QUEUE_NAME",
    # Legacy Queue classes
    "AtLeastOnceQueue",
    "AtMostOnceQueue",
    "Queue",
    "BaseQueue",
    # Legacy decorators
    "legacy_task_decorator",
    "repeat",
    "retry",
    "AsyncTask",
    "JobMeta",
    # Backward compatibility
    "legacy_task",
    "migrate_task",
    "LegacyJob",
    "LegacyQueue",
    # Exceptions
    "PgqException",
    "PgqIncorrectQueue",
    "PgqNoDefinedQueue",
    # Legacy worker
    "Worker",
    # Django 6.0 django.tasks API
    "PostgresQueueBackend",
    "Task",
    "TaskResult",
    "TaskError",
    "ResultStatus",
    "InvalidTask",
    "TaskResultDoesNotExist",
    "task",
    "get_task_backend",
    "configure_backend",
    "get_default_backend",
]
