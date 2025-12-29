"""
django-pg-queue - PostgreSQL-backed task queue for Django 6.0+.

This package implements the django.tasks API using PostgreSQL as the
backend, leveraging SELECT FOR UPDATE SKIP LOCKED for efficient,
non-blocking task claiming.

Usage:
    from pgq import task, PostgresQueueBackend, TaskResult

    @task(priority=10, queue_name='emails')
    def send_email(recipient, subject, body):
        # task implementation
        pass

    # Enqueue a task
    result = send_email.enqueue('user@example.com', 'Hello', 'World')

    # Check result
    result = get_task_backend().get_result(result.id)
"""

__version__ = "0.9.0"


def __getattr__(name):
    """Lazy import for top-level package attributes."""
    # Models
    if name in ("Job", "BaseJob", "TaskResultStatus", "DEFAULT_QUEUE_NAME"):
        from pgq import models
        return getattr(models, name)

    # Exceptions
    if name in ("PgqException",):
        from pgq import exceptions
        return getattr(exceptions, name)

    # Django tasks API
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
    # Exceptions
    "PgqException",
    # Django tasks API
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
