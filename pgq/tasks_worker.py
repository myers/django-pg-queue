"""
Django 6.0 django.tasks compatible worker command.

This worker processes tasks enqueued via the PostgresQueueBackend,
maintaining status tracking throughout the task lifecycle.

Usage:
    python manage.py pgq_worker [--backend default] [--listen] [--delay 1]
"""

import logging
import os
import signal
import time
from typing import Any, Optional, Set

from django.core.management.base import BaseCommand
from django.db import connection

from .backend import PostgresQueueBackend, get_task_backend
from .exceptions import PgqException


class DjangoTasksWorker(BaseCommand):
    """
    Worker command for processing django.tasks compatible tasks.

    This worker uses the PostgresQueueBackend to claim and execute tasks
    while maintaining proper status tracking.
    """

    help = "Run a worker to process background tasks"
    logger = logging.getLogger(__name__)

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument(
            "--backend",
            type=str,
            default="default",
            help="The task backend alias to use (default: 'default')",
        )
        parser.add_argument(
            "--delay",
            type=float,
            default=1,
            help="Seconds to wait between polling for tasks (default: 1)",
        )
        parser.add_argument(
            "--listen",
            action="store_true",
            help="Use PostgreSQL LISTEN/NOTIFY for efficient task notification",
        )
        parser.add_argument(
            "--worker-id",
            type=str,
            default=None,
            help="Unique identifier for this worker (default: auto-generated)",
        )

    def handle(self, **options: Any) -> None:
        self._shutdown = False
        self._in_task = False

        self.delay: float = options["delay"]
        self.listen: bool = options["listen"]
        self.backend_alias: str = options["backend"]
        self.worker_id: str = options.get("worker_id") or f"pgq#{os.getpid()}"

        # Get the backend
        self.backend: PostgresQueueBackend = get_task_backend(self.backend_alias)

        # Set PostgreSQL application name for monitoring
        with connection.cursor() as cursor:
            cursor.execute(
                "SET application_name TO %s",
                [self.worker_id],
            )

        # Set up LISTEN if requested
        if self.listen:
            if not self.backend.notify_channel:
                self.logger.warning(
                    "LISTEN mode requested but backend has no notify_channel configured. "
                    "Falling back to polling."
                )
                self.listen = False
            else:
                self.backend.listen()

        self.logger.info(
            "Starting worker %s for backend '%s' (queue: %s, mode: %s)",
            self.worker_id,
            self.backend_alias,
            self.backend.queue_name,
            "listen" if self.listen else "poll",
        )

        try:
            # Set up signal handlers for graceful shutdown
            signal.signal(signal.SIGINT, self.handle_shutdown)
            signal.signal(signal.SIGTERM, self.handle_shutdown)

            # Main loop
            while True:
                self.run_available_tasks()
                self.wait()

        except InterruptedError:
            self.logger.info("Worker %s shutting down gracefully", self.worker_id)

    def handle_shutdown(self, sig: Any, frame: Any) -> None:
        """Handle shutdown signals gracefully."""
        if self._in_task:
            self.logger.info(
                "Shutdown signal received. Waiting for current task to finish..."
            )
            self._shutdown = True
        else:
            raise InterruptedError

    def run_available_tasks(self) -> None:
        """Process all available tasks until queue is empty."""
        failed_task_ids: Set[int] = set()

        while True:
            self._in_task = True
            try:
                result = self.backend.run_once(
                    exclude_ids=list(failed_task_ids) if failed_task_ids else None,
                    worker_id=self.worker_id,
                )

                if result is None:
                    # No more tasks available
                    self._in_task = False
                    return

                self.logger.info(
                    "Completed task %s (status: %s, attempts: %d)",
                    result.id,
                    result.status,
                    result.attempts,
                )

            except PgqException as e:
                if e.job is not None:
                    self.logger.exception(
                        "Error processing job %s: %s",
                        e.job.id,
                        e,
                        extra={"data": {"job": e.job.to_json()}},
                    )
                    failed_task_ids.add(e.job.id)
                else:
                    raise

            except Exception as e:
                self.logger.exception("Unexpected error in worker: %s", e)
                # Continue processing other tasks

            finally:
                self._in_task = False

            if self._shutdown:
                raise InterruptedError

    def wait(self) -> int:
        """Wait for new tasks or timeout."""
        if self.listen and self.backend.notify_channel:
            return self._wait_listen()
        else:
            return self._wait_poll()

    def _wait_listen(self) -> int:
        """Wait using PostgreSQL LISTEN/NOTIFY."""
        import select

        connection.connection.poll()

        # Check for pending notifications
        notifies = [
            n for n in connection.connection.notifies
            if n.channel == self.backend.notify_channel
        ]

        if notifies:
            # Clear processed notifications
            connection.connection.notifies = [
                n for n in connection.connection.notifies
                if n.channel != self.backend.notify_channel
            ]
            return len(notifies)

        # Wait for notification or timeout
        select.select([connection.connection], [], [], self.delay)
        connection.connection.poll()

        notifies = [
            n for n in connection.connection.notifies
            if n.channel == self.backend.notify_channel
        ]
        connection.connection.notifies = [
            n for n in connection.connection.notifies
            if n.channel != self.backend.notify_channel
        ]

        count = len(notifies)
        self.logger.debug("Woke up with %d NOTIFY signals", count)
        return count

    def _wait_poll(self) -> int:
        """Wait using simple polling."""
        time.sleep(self.delay)
        return 1


class Command(DjangoTasksWorker):
    """
    Management command entry point.

    Usage:
        python manage.py pgq_worker
        python manage.py pgq_worker --backend myqueue
        python manage.py pgq_worker --listen
        python manage.py pgq_worker --delay 5
    """
    pass
