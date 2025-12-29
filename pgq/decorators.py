import copy
import datetime
from dataclasses import dataclass
import functools
import logging
import random
from typing import Any, Callable, Dict, Optional, Type, Union, TYPE_CHECKING

from django.db import transaction

if TYPE_CHECKING:
    from .queue import Queue
    from .models import BaseJob
    from .backend import Task as DjangoTask

    DelayFnType = Callable[[int], datetime.timedelta]
    TaskFnType = Callable[[Queue, BaseJob], Any]
else:
    Queue = None
    BaseJob = None
    DjangoTask = None
    DelayFnType = None
    TaskFnType = None


def repeat(delay: datetime.timedelta) -> Callable[..., Any]:
    """
    Endlessly repeats a task, every `delay` (a timedelta).

    Under at-least-once delivery, the tasks can not overlap. The next scheduled
    task only becomes visible once the previous one commits.

        @repeat(datetime.timedelta(minutes=5)
        def task(queue, job):
            pass

    This will run `task` every 5 minutes. It's up to you to kick off the first
    task, though.
    """

    def decorator(fn: TaskFnType) -> TaskFnType:
        def inner(queue: Queue, job: BaseJob) -> Any:
            queue.enqueue(
                job.task,
                job.args,
                execute_at=job.execute_at + delay,
                priority=job.priority,
            )
            return fn(queue, job)

        return inner

    return decorator


def exponential_with_jitter(offset: int = 6) -> DelayFnType:
    def delayfn(retries: int) -> datetime.timedelta:
        jitter = random.randrange(-15, 15)
        return datetime.timedelta(seconds=2 ** (retries + offset) + jitter)

    return delayfn


def retry(
    max_retries: int = 0,
    delay_offset_seconds: int = 5,
    delayfn: Optional[DelayFnType] = None,
    Exc: Type[Exception] = Exception,
    on_failure: Optional[
        Callable[[Queue, BaseJob, Any, "JobMeta", Exception], Any]
    ] = None,
    on_success: Optional[Callable[[BaseJob, Any], Any]] = None,
    JobMetaType: Optional[Type["JobMeta"]] = None,
):
    if delayfn is None:
        delayfn = exponential_with_jitter(delay_offset_seconds)
    if JobMetaType is None:
        JobMetaType = JobMeta

    def decorator(fn):
        logger = logging.getLogger(__name__)

        @functools.wraps(fn)
        def inner(queue, job):
            original_job_id = job.args["meta"].setdefault("job_id", job.id)

            try:
                args = copy.deepcopy(job.args)
                with transaction.atomic():
                    result = fn(
                        queue, job, args["func_args"], JobMetaType(**args["meta"])
                    )
            except Exc as e:
                retries = job.args["meta"].get("retries", 0)
                if retries < max_retries:
                    job.args["meta"].update(
                        {"retries": retries + 1, "job_id": original_job_id}
                    )
                    delay = delayfn(retries)
                    job.execute_at += delay
                    job.id = None
                    job.save(force_insert=True)
                    logger.warning(
                        "Task %r failed: %s. Retrying in %s.",
                        job,
                        e,
                        delay,
                        exc_info=True,
                    )
                else:
                    if on_failure:
                        args = copy.deepcopy(job.args)
                        return on_failure(
                            queue,
                            job,
                            args["func_args"],
                            JobMetaType(**args["meta"]),
                            error=e,
                        )
                    logger.exception(
                        "Task %r exceeded its retry limit: %s.", job, e, exc_info=True
                    )
            else:
                if on_success is not None:
                    on_success(job, result)
                return result

        return inner

    return decorator


class AsyncTask:
    """
    A useful standin for celery async tasks.

    Represents an async task, can be used like so:

    @task
    def increment_followers(...): ...

    increment_followers.enqueue(...)
    """

    def __init__(self, queue: Queue, name: str):
        self.queue = queue
        self.name = name

    def enqueue(
        self, args: Dict[str, Any], meta: Optional[Dict[str, Any]] = None
    ) -> BaseJob:
        wrapped_args = {"func_args": args, "meta": meta if meta is not None else {}}
        return self.queue.enqueue(self.name, wrapped_args)

    def __str__(self) -> str:
        return f"AsyncTask({self.queue.notify_channel}, {self.name})"


def task(
    queue: Queue,
    max_retries: int = 0,
    delay_offset_seconds: int = 5,
    on_failure: Optional[Callable[..., Any]] = None,
    JobMetaType: Optional[Type["JobMeta"]] = None,
) -> Callable[..., Any]:
    """
    Decorator to register the task to the queue.

    @task(queuename, max_retries=5)

    delay_offset_seconds:
        5th retry will take half hour at 5; delay (seconds) = 2 ** (retry + offset)
    """
    if JobMetaType is None:
        JobMetaType = JobMeta

    def register(fn: Callable[..., Any]) -> AsyncTask:
        name = fn.__name__
        assert name not in queue.tasks
        queue.tasks[name] = retry(
            max_retries=max_retries,
            delay_offset_seconds=delay_offset_seconds,
            on_failure=on_failure,
            JobMetaType=JobMetaType,
        )(fn)
        return AsyncTask(queue, name)

    return register


@dataclass
class JobMeta:
    job_id: int
    retries: int = 0


# -----------------------------------------------------------------------------
# Backward Compatibility Layer for django.tasks Migration
# -----------------------------------------------------------------------------


@dataclass
class LegacyJob:
    """
    A wrapper that provides backward compatibility for legacy pgq task signatures.

    Legacy pgq tasks expect `(queue, job)` parameters where job has an `args` dict.
    Django tasks pass arguments directly to the function. This class wraps the
    new-style arguments in a job-like object.
    """
    id: int
    args: Dict[str, Any]
    kwargs: Dict[str, Any]
    task: str = ""
    priority: int = 0
    queue: str = "default"
    created_at: Optional[datetime.datetime] = None
    execute_at: Optional[datetime.datetime] = None

    @classmethod
    def from_args_kwargs(
        cls,
        args: tuple = (),
        kwargs: Optional[Dict[str, Any]] = None,
        job_id: int = 0,
        task_name: str = "",
    ) -> "LegacyJob":
        """Create a LegacyJob from django.tasks style arguments."""
        if kwargs is None:
            kwargs = {}
        return cls(
            id=job_id,
            args={"args": list(args), "kwargs": kwargs},
            kwargs=kwargs,
            task=task_name,
        )


class LegacyQueue:
    """
    A minimal queue-like object for backward compatibility.

    Provides the interface expected by legacy pgq tasks when running
    in django.tasks mode.
    """

    def __init__(
        self,
        queue_name: str = "default",
        notify_channel: Optional[str] = None,
        backend_alias: str = "default",
    ):
        self.queue = queue_name
        self.queue_name = queue_name
        self.notify_channel = notify_channel
        self._backend_alias = backend_alias

    def enqueue(
        self,
        task: str,
        args: Optional[Dict[str, Any]] = None,
        execute_at: Optional[datetime.datetime] = None,
        priority: Optional[int] = None,
    ) -> Any:
        """Enqueue a task using the django.tasks backend."""
        from .backend import get_task_backend

        backend = get_task_backend(self._backend_alias)

        # Create a temporary Task object for this enqueue
        from .backend import Task as DjangoTask

        # For legacy compatibility, create a simple callable wrapper
        task_obj = DjangoTask(
            func=lambda *a, **kw: None,  # Placeholder
            priority=priority or 0,
            queue_name=self.queue_name,
            backend=self._backend_alias,
            run_after=execute_at,
        )
        task_obj.name = task  # Override the name

        return backend.enqueue(task_obj, args=(), kwargs=args or {})


def legacy_task(
    queue: Queue,
    backend: str = "default",
) -> Callable[[TaskFnType], "DjangoTask"]:
    """
    Decorator providing backward compatibility for legacy pgq task signatures.

    This decorator wraps a legacy-style task function `(queue, job)` and
    registers it with the django.tasks backend, allowing gradual migration.

    Usage:
        # Old code continues to work:
        @legacy_task(queue)
        def send_email(queue, job):
            recipient = job.args['recipient']
            # ... task implementation

        # Can still be called the old way internally, but enqueued via django.tasks:
        send_email.enqueue(recipient='user@example.com')

    Args:
        queue: The legacy Queue instance (used for queue_name and notify_channel)
        backend: The django.tasks backend alias to use

    Returns:
        A django.tasks Task object with an enqueue() method
    """
    from .backend import Task as DjangoTask, get_task_backend

    def decorator(fn: TaskFnType) -> DjangoTask:
        # Create a wrapper that converts new-style to old-style
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Create a legacy-compatible job object
            legacy_job = LegacyJob.from_args_kwargs(
                args=args,
                kwargs=kwargs,
                task_name=fn.__name__,
            )

            # Create a legacy-compatible queue object
            legacy_queue = LegacyQueue(
                queue_name=queue.queue if hasattr(queue, 'queue') else "default",
                notify_channel=getattr(queue, 'notify_channel', None),
                backend_alias=backend,
            )

            # Call the legacy function with old-style arguments
            return fn(legacy_queue, legacy_job)

        # Create a django.tasks Task
        task_obj = DjangoTask(
            func=wrapper,
            priority=0,
            queue_name=queue.queue if hasattr(queue, 'queue') else "default",
            backend=backend,
        )

        # Also register with the legacy queue for backward compatibility
        if hasattr(queue, 'tasks'):
            queue.tasks[fn.__name__] = lambda q, j: fn(q, j)

        return task_obj

    return decorator


def migrate_task(
    legacy_queue: Queue,
    backend: str = "default",
    priority: int = 0,
    queue_name: Optional[str] = None,
) -> Callable[[Callable[..., Any]], "DjangoTask"]:
    """
    Decorator to migrate a task from legacy pgq to django.tasks format.

    Unlike legacy_task(), this decorator expects the new django.tasks
    function signature (direct arguments, not queue/job).

    Usage:
        # New-style task that works with django.tasks backend:
        @migrate_task(queue, priority=10)
        def send_email(recipient, subject, body):
            # Direct parameter access
            pass

        # Enqueue using django.tasks style:
        result = send_email.enqueue('user@example.com', 'Hello', 'World')

        # Or with keyword arguments:
        result = send_email.enqueue(
            recipient='user@example.com',
            subject='Hello',
            body='World'
        )

    Args:
        legacy_queue: The legacy Queue instance (used to derive queue_name)
        backend: The django.tasks backend alias
        priority: Default priority for the task
        queue_name: Override queue name (default: from legacy_queue)

    Returns:
        A django.tasks Task object
    """
    from .backend import Task as DjangoTask

    effective_queue_name = queue_name or (
        legacy_queue.queue if hasattr(legacy_queue, 'queue') else "default"
    )

    def decorator(fn: Callable[..., Any]) -> DjangoTask:
        return DjangoTask(
            func=fn,
            priority=priority,
            queue_name=effective_queue_name,
            backend=backend,
        )

    return decorator
