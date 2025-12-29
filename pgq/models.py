from typing import Any, Dict, Iterable, Optional, Sequence, Type, TypeVar

from django.db import models
from django.db import connection
from django.contrib.postgres.functions import TransactionNow
from django.utils import timezone

try:
    from django.db.models import JSONField
except ImportError:
    from django.contrib.postgres.fields import JSONField  # type: ignore[misc]

DEFAULT_QUEUE_NAME = "default"


class TaskResultStatus:
    """Status enum for django.tasks compatibility."""
    READY = "READY"
    RUNNING = "RUNNING"
    SUCCESSFUL = "SUCCESSFUL"
    FAILED = "FAILED"

    CHOICES = [
        (READY, "Ready"),
        (RUNNING, "Running"),
        (SUCCESSFUL, "Successful"),
        (FAILED, "Failed"),
    ]


_Self = TypeVar("_Self", bound="BaseJob")


class BaseJob(models.Model):
    # Original fields (preserved for backward compatibility)
    id = models.BigAutoField(primary_key=True)
    created_at = models.DateTimeField(default=TransactionNow)
    execute_at = models.DateTimeField(default=TransactionNow)
    priority = models.IntegerField(
        default=0, help_text="Jobs with higher priority will be processed first."
    )
    task = models.CharField(max_length=255)
    args = JSONField(default=dict)
    queue = models.CharField(
        max_length=32,
        default=DEFAULT_QUEUE_NAME,
        help_text="Use a unique name to represent each queue.",
    )

    # New fields for django.tasks compatibility
    kwargs = JSONField(
        default=dict,
        help_text="Keyword arguments for django.tasks style tasks.",
    )
    status = models.CharField(
        max_length=20,
        default=TaskResultStatus.READY,
        choices=TaskResultStatus.CHOICES,
        db_index=True,
        help_text="Current status of the task.",
    )
    started_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When task execution started.",
    )
    finished_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When task execution completed.",
    )
    return_value = JSONField(
        null=True,
        blank=True,
        help_text="Return value from successful task execution.",
    )
    error_traceback = models.TextField(
        null=True,
        blank=True,
        help_text="Full traceback if task failed.",
    )
    error_class = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text="Exception class name if task failed.",
    )
    attempts = models.IntegerField(
        default=0,
        help_text="Number of execution attempts.",
    )
    worker_id = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text="Identifier of the worker processing this task.",
    )

    class Meta:
        abstract = True
        indexes = [
            models.Index(fields=["-priority", "created_at"]),
            models.Index(fields=["queue"]),
            models.Index(fields=["status", "queue"]),
        ]

    def __str__(self) -> str:
        return "%s: %s" % (self.id, self.task)

    @classmethod
    def dequeue(
        cls: Type[_Self],
        exclude_ids: Optional[Iterable[int]] = None,
        tasks: Optional[Sequence[str]] = None,
        queue: str = DEFAULT_QUEUE_NAME,
    ) -> Optional[_Self]:
        """
        Claims the first available task and returns it. If there are no
        tasks available, returns None.

        exclude_ids: Iterable[int] - excludes jobs with these ids
        tasks: Optional[Sequence[str]] - filters by jobs with these tasks.

        For at-most-once delivery, commit the transaction before
        processing the task. For at-least-once delivery, dequeue and
        finish processing the task in the same transaction.

        To put a job back in the queue, you can just call
        .save(force_insert=True) on the returned object.
        """

        WHERE = "WHERE execute_at <= now() AND NOT id = ANY(%s) AND queue = %s"
        args = [[] if exclude_ids is None else list(exclude_ids), queue]
        if tasks is not None:
            WHERE += " AND TASK = ANY(%s)"
            args.append(tasks)

        jobs = list(
            cls.objects.raw(
                """
            DELETE FROM {db_table}
            WHERE id = (
                SELECT id
                FROM {db_table}
                {WHERE}
                ORDER BY priority DESC, created_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING *;
            """.format(
                    db_table=connection.ops.quote_name(cls._meta.db_table), WHERE=WHERE
                ),
                args,
            )
        )
        assert len(jobs) <= 1
        if jobs:
            return jobs[0]
        else:
            return None

    @classmethod
    def claim(
        cls: Type[_Self],
        exclude_ids: Optional[Iterable[int]] = None,
        tasks: Optional[Sequence[str]] = None,
        queue: str = DEFAULT_QUEUE_NAME,
        worker_id: Optional[str] = None,
    ) -> Optional[_Self]:
        """
        Claims the first available task without deleting it. Updates status
        to RUNNING. For use with django.tasks compatible processing where
        we need to track task state.

        exclude_ids: Iterable[int] - excludes jobs with these ids
        tasks: Optional[Sequence[str]] - filters by jobs with these tasks.
        worker_id: Optional[str] - identifier for the claiming worker

        Returns the claimed Job or None if no jobs available.
        """
        WHERE = "WHERE execute_at <= now() AND NOT id = ANY(%s) AND queue = %s AND status = %s"
        args = [
            [] if exclude_ids is None else list(exclude_ids),
            queue,
            TaskResultStatus.READY,
        ]
        if tasks is not None:
            WHERE += " AND TASK = ANY(%s)"
            args.append(tasks)

        # Build the UPDATE query with optional worker_id
        update_fields = "status = %s, started_at = now(), attempts = attempts + 1"
        update_args = [TaskResultStatus.RUNNING]
        if worker_id:
            update_fields += ", worker_id = %s"
            update_args.append(worker_id)

        jobs = list(
            cls.objects.raw(
                """
            UPDATE {db_table}
            SET {update_fields}
            WHERE id = (
                SELECT id
                FROM {db_table}
                {WHERE}
                ORDER BY priority DESC, created_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING *;
            """.format(
                    db_table=connection.ops.quote_name(cls._meta.db_table),
                    update_fields=update_fields,
                    WHERE=WHERE,
                ),
                update_args + args,
            )
        )
        assert len(jobs) <= 1
        if jobs:
            return jobs[0]
        else:
            return None

    def mark_successful(self, return_value: Any = None) -> None:
        """Mark this job as successfully completed."""
        self.status = TaskResultStatus.SUCCESSFUL
        self.finished_at = timezone.now()
        self.return_value = return_value
        self.save(update_fields=["status", "finished_at", "return_value"])

    def mark_failed(self, error_traceback: str, error_class: str) -> None:
        """Mark this job as failed."""
        self.status = TaskResultStatus.FAILED
        self.finished_at = timezone.now()
        self.error_traceback = error_traceback
        self.error_class = error_class
        self.save(update_fields=["status", "finished_at", "error_traceback", "error_class"])

    def reset_for_retry(self) -> None:
        """Reset job status to READY for retry (used with AtLeastOnceQueue)."""
        self.status = TaskResultStatus.READY
        self.started_at = None
        self.finished_at = None
        self.worker_id = None
        self.save(update_fields=["status", "started_at", "finished_at", "worker_id"])

    def to_json(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "created_at": self.created_at,
            "execute_at": self.execute_at,
            "priority": self.priority,
            "queue": self.queue,
            "task": self.task,
            "args": self.args,
            "kwargs": self.kwargs,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "return_value": self.return_value,
            "error_traceback": self.error_traceback,
            "error_class": self.error_class,
            "attempts": self.attempts,
            "worker_id": self.worker_id,
        }


class Job(BaseJob):
    """pgq builtin Job model"""
