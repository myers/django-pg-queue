"""
Django 6.0 django.tasks compatible backend for PostgreSQL queue.

This module provides a backend implementation that follows Django's
BaseTaskBackend interface while preserving the PostgreSQL optimizations
(SELECT FOR UPDATE SKIP LOCKED, transactional processing, NOTIFY/LISTEN).

Usage in settings.py:

    TASKS = {
        'default': {
            'BACKEND': 'pgq.backend.PostgresQueueBackend',
            'OPTIONS': {
                'queue': 'default',
                'notify_channel': 'default',
                'at_least_once': True,
            }
        }
    }
"""

import asyncio
import importlib
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Type, Union

from django.db import connection, transaction
from django.utils import timezone

from .models import Job, TaskResultStatus, DEFAULT_QUEUE_NAME


# -----------------------------------------------------------------------------
# Django Tasks API Compatibility Layer
# -----------------------------------------------------------------------------
# These classes mirror Django 6.0's django.tasks API. When Django 6.0 is
# released, these can be replaced with imports from django.tasks.


@dataclass
class TaskError:
    """Represents an error that occurred during task execution."""
    traceback: str
    exception_class: str
    message: str = ""


class ResultStatus:
    """Status values for TaskResult."""
    READY = "READY"
    RUNNING = "RUNNING"
    SUCCESSFUL = "SUCCESSFUL"
    FAILED = "FAILED"


@dataclass
class TaskResult:
    """
    Represents the result of a task execution.

    This class mirrors Django 6.0's TaskResult interface.
    """
    id: str
    task: Optional["Task"]
    status: str
    enqueued_at: Optional[datetime]
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    return_value: Any = None
    errors: List[TaskError] = field(default_factory=list)
    attempts: int = 0
    backend: Optional[str] = None

    @property
    def is_finished(self) -> bool:
        """Check if the task has finished (successfully or failed)."""
        return self.status in (ResultStatus.SUCCESSFUL, ResultStatus.FAILED)

    @property
    def is_successful(self) -> bool:
        """Check if the task completed successfully."""
        return self.status == ResultStatus.SUCCESSFUL

    @property
    def is_failed(self) -> bool:
        """Check if the task failed."""
        return self.status == ResultStatus.FAILED


class InvalidTask(Exception):
    """Raised when a task cannot be enqueued."""
    pass


class TaskResultDoesNotExist(Exception):
    """Raised when a task result cannot be found."""
    pass


class Task:
    """
    Represents a task that can be enqueued for background execution.

    This class mirrors Django 6.0's Task interface.
    """

    def __init__(
        self,
        func: Callable,
        priority: int = 0,
        queue_name: Optional[str] = None,
        backend: str = "default",
        run_after: Optional[Union[datetime, timedelta]] = None,
    ):
        self.func = func
        self.priority = priority
        self.queue_name = queue_name
        self.backend_name = backend
        self.run_after = run_after

        # Store the fully qualified name for serialization
        self.name = f"{func.__module__}.{func.__qualname__}"

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Execute the task synchronously."""
        return self.func(*args, **kwargs)

    def enqueue(self, *args: Any, **kwargs: Any) -> TaskResult:
        """Enqueue this task for background execution."""
        backend = get_task_backend(self.backend_name)
        return backend.enqueue(self, args, kwargs)

    def using(
        self,
        priority: Optional[int] = None,
        queue_name: Optional[str] = None,
        run_after: Optional[Union[datetime, timedelta]] = None,
    ) -> "Task":
        """Return a new Task with modified options."""
        return Task(
            func=self.func,
            priority=priority if priority is not None else self.priority,
            queue_name=queue_name if queue_name is not None else self.queue_name,
            backend=self.backend_name,
            run_after=run_after if run_after is not None else self.run_after,
        )


def task(
    priority: int = 0,
    queue_name: Optional[str] = None,
    backend: str = "default",
    run_after: Optional[Union[datetime, timedelta]] = None,
) -> Callable[[Callable], Task]:
    """
    Decorator to define a task compatible with django.tasks.

    Usage:
        @task(priority=10, queue_name='emails')
        def send_email(recipient, subject, body):
            # task implementation
            pass

        # Enqueue the task
        result = send_email.enqueue('user@example.com', 'Hello', 'World')
    """
    def decorator(func: Callable) -> Task:
        return Task(
            func=func,
            priority=priority,
            queue_name=queue_name,
            backend=backend,
            run_after=run_after,
        )
    return decorator


# -----------------------------------------------------------------------------
# Backend Registry
# -----------------------------------------------------------------------------

_backends: Dict[str, "PostgresQueueBackend"] = {}


def get_task_backend(alias: str = "default") -> "PostgresQueueBackend":
    """Get a task backend by alias."""
    if alias not in _backends:
        # Try to load from Django settings
        from django.conf import settings
        tasks_config = getattr(settings, "TASKS", {})

        if alias in tasks_config:
            config = tasks_config[alias]
            backend_path = config.get("BACKEND", "pgq.backend.PostgresQueueBackend")
            options = config.get("OPTIONS", {})

            # Import the backend class
            module_path, class_name = backend_path.rsplit(".", 1)
            module = importlib.import_module(module_path)
            backend_class = getattr(module, class_name)

            _backends[alias] = backend_class(alias=alias, options=options)
        else:
            # Default backend with default options
            _backends[alias] = PostgresQueueBackend(alias=alias)

    return _backends[alias]


def configure_backend(alias: str, backend: "PostgresQueueBackend") -> None:
    """Manually configure a backend (useful for testing)."""
    _backends[alias] = backend


# -----------------------------------------------------------------------------
# PostgreSQL Queue Backend
# -----------------------------------------------------------------------------

class PostgresQueueBackend:
    """
    Django 6.0 django.tasks compatible backend using PostgreSQL.

    This backend leverages PostgreSQL's SELECT FOR UPDATE SKIP LOCKED
    for efficient, non-blocking task claiming while providing the
    TaskResult interface required by django.tasks.

    Attributes:
        supports_defer: Whether run_after parameter is supported (True)
        supports_async_task: Whether async tasks are supported (False)
        supports_get_result: Whether results can be retrieved (True)
        supports_priority: Whether priority ordering is supported (True)
    """

    supports_defer = True
    supports_async_task = False
    supports_get_result = True
    supports_priority = True

    def __init__(
        self,
        alias: str = "default",
        options: Optional[Dict[str, Any]] = None,
    ):
        self.alias = alias
        self.options = options or {}
        self.queue_name = self.options.get("queue", DEFAULT_QUEUE_NAME)
        self.notify_channel = self.options.get("notify_channel")
        self.at_least_once = self.options.get("at_least_once", True)
        self._task_registry: Dict[str, Task] = {}

    def enqueue(
        self,
        task_obj: Task,
        args: tuple = (),
        kwargs: Optional[Dict[str, Any]] = None,
    ) -> TaskResult:
        """
        Enqueue a task for later execution.

        Args:
            task_obj: The Task object to enqueue
            args: Positional arguments for the task
            kwargs: Keyword arguments for the task

        Returns:
            TaskResult with status READY
        """
        if kwargs is None:
            kwargs = {}

        # Validate the task
        self.validate_task(task_obj)

        # Calculate execute_at from run_after
        execute_at = self._get_execute_at(task_obj)

        # Determine queue name
        queue = task_obj.queue_name or self.queue_name

        # Create the job
        job = Job.objects.create(
            task=self._get_task_path(task_obj),
            args=list(args),
            kwargs=kwargs,
            priority=self._normalize_priority(task_obj.priority),
            queue=queue,
            execute_at=execute_at,
            status=TaskResultStatus.READY,
        )

        # Register the task for later lookup
        self._task_registry[self._get_task_path(task_obj)] = task_obj

        # Send PostgreSQL NOTIFY if configured
        if self.notify_channel:
            self.notify()

        return self._job_to_result(job, task_obj)

    def get_result(self, result_id: str) -> TaskResult:
        """
        Retrieve a task result by ID.

        Args:
            result_id: The ID of the task result

        Returns:
            TaskResult for the given ID

        Raises:
            TaskResultDoesNotExist: If no result with that ID exists
        """
        try:
            job = Job.objects.get(id=result_id)
            task_obj = self._task_registry.get(job.task)
            return self._job_to_result(job, task_obj)
        except Job.DoesNotExist:
            raise TaskResultDoesNotExist(f"No result with id {result_id}")

    def validate_task(self, task_obj: Task) -> None:
        """
        Validate whether a Task can be enqueued.

        Args:
            task_obj: The Task to validate

        Raises:
            InvalidTask: If the task cannot be enqueued
        """
        if asyncio.iscoroutinefunction(task_obj.func):
            raise InvalidTask("PostgresQueueBackend does not support async tasks")

        if task_obj.priority < -100 or task_obj.priority > 100:
            raise InvalidTask("Priority must be between -100 and 100")

    def _normalize_priority(self, priority: int) -> int:
        """
        Map Django's -100 to 100 range to internal priority.

        Both Django and pgq use "higher = sooner" convention,
        so direct mapping works.
        """
        return priority

    def _get_execute_at(self, task_obj: Task) -> datetime:
        """Convert run_after to execute_at datetime."""
        if task_obj.run_after is None:
            return timezone.now()
        elif isinstance(task_obj.run_after, timedelta):
            return timezone.now() + task_obj.run_after
        else:
            return task_obj.run_after

    def _get_task_path(self, task_obj: Task) -> str:
        """Get fully qualified task name for storage."""
        return task_obj.name

    def _job_to_result(
        self,
        job: Job,
        task_obj: Optional[Task] = None,
    ) -> TaskResult:
        """Convert Job model instance to TaskResult."""
        errors = []
        if job.error_traceback:
            errors.append(TaskError(
                traceback=job.error_traceback,
                exception_class=job.error_class or "Exception",
                message=job.error_traceback.split("\n")[-2] if job.error_traceback else "",
            ))

        return TaskResult(
            id=str(job.id),
            task=task_obj,
            status=job.status,
            enqueued_at=job.created_at,
            started_at=job.started_at,
            finished_at=job.finished_at,
            return_value=job.return_value if job.status == TaskResultStatus.SUCCESSFUL else None,
            errors=errors,
            attempts=job.attempts,
            backend=self.alias,
        )

    def notify(self) -> None:
        """Send PostgreSQL NOTIFY signal."""
        if self.notify_channel:
            with connection.cursor() as cursor:
                cursor.execute('NOTIFY "%s";' % self.notify_channel)

    def listen(self) -> None:
        """Register to listen for NOTIFY signals."""
        if not self.notify_channel:
            raise ValueError("notify_channel must be set to use listen()")
        with connection.cursor() as cursor:
            cursor.execute('LISTEN "{}";'.format(self.notify_channel))

    # -------------------------------------------------------------------------
    # Task Execution (for workers)
    # -------------------------------------------------------------------------

    def claim_job(
        self,
        exclude_ids: Optional[List[int]] = None,
        worker_id: Optional[str] = None,
    ) -> Optional[Job]:
        """
        Claim a job for processing.

        Uses SELECT FOR UPDATE SKIP LOCKED for non-blocking claims.

        Args:
            exclude_ids: Job IDs to exclude from claiming
            worker_id: Identifier for this worker

        Returns:
            The claimed Job or None if no jobs available
        """
        return Job.claim(
            exclude_ids=exclude_ids,
            queue=self.queue_name,
            worker_id=worker_id,
        )

    def load_task(self, task_path: str) -> Callable:
        """
        Load a task function from its module path.

        Args:
            task_path: Fully qualified function path (e.g., 'myapp.tasks.send_email')

        Returns:
            The task callable
        """
        # First check the registry
        if task_path in self._task_registry:
            return self._task_registry[task_path].func

        # Otherwise import it
        module_path, func_name = task_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        func_or_task = getattr(module, func_name)

        # Handle both Task objects and plain functions
        if isinstance(func_or_task, Task):
            self._task_registry[task_path] = func_or_task
            return func_or_task.func
        else:
            return func_or_task

    def execute_job(self, job: Job, worker_id: Optional[str] = None) -> Any:
        """
        Execute a claimed job.

        Args:
            job: The Job to execute
            worker_id: Identifier for this worker

        Returns:
            The return value from the task function
        """
        try:
            # Load and execute the task
            task_func = self.load_task(job.task)
            result = task_func(*job.args, **job.kwargs)

            # Mark successful
            job.mark_successful(return_value=result)

            return result

        except Exception as e:
            # Mark failed
            job.mark_failed(
                error_traceback=traceback.format_exc(),
                error_class=type(e).__name__,
            )
            raise

    def run_once(
        self,
        exclude_ids: Optional[List[int]] = None,
        worker_id: Optional[str] = None,
    ) -> Optional[TaskResult]:
        """
        Claim and execute one task.

        For AtLeastOnceQueue behavior, wrap this in a transaction that
        will rollback on failure, resetting the job to READY status.

        Args:
            exclude_ids: Job IDs to exclude
            worker_id: Identifier for this worker

        Returns:
            TaskResult if a job was processed, None otherwise
        """
        if self.at_least_once:
            return self._run_once_at_least_once(exclude_ids, worker_id)
        else:
            return self._run_once_at_most_once(exclude_ids, worker_id)

    def _run_once_at_least_once(
        self,
        exclude_ids: Optional[List[int]] = None,
        worker_id: Optional[str] = None,
    ) -> Optional[TaskResult]:
        """Execute with at-least-once semantics (uses transaction)."""
        job = None
        try:
            with transaction.atomic():
                job = self.claim_job(exclude_ids, worker_id)
                if not job:
                    return None

                self.execute_job(job, worker_id)
                return self._job_to_result(job)
        except Exception:
            # On failure, reset job for retry if it exists
            if job:
                try:
                    job.refresh_from_db()
                    job.reset_for_retry()
                except Exception:
                    pass
            raise

    def _run_once_at_most_once(
        self,
        exclude_ids: Optional[List[int]] = None,
        worker_id: Optional[str] = None,
    ) -> Optional[TaskResult]:
        """Execute with at-most-once semantics (no transaction wrap)."""
        job = self.claim_job(exclude_ids, worker_id)
        if not job:
            return None

        try:
            self.execute_job(job, worker_id)
        except Exception:
            # Job stays failed, no retry
            pass

        return self._job_to_result(job)


# Convenience alias for default backend access
default_task_backend = None


def get_default_backend() -> PostgresQueueBackend:
    """Get the default task backend."""
    global default_task_backend
    if default_task_backend is None:
        default_task_backend = get_task_backend("default")
    return default_task_backend
