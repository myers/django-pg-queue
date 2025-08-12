import fcntl
import logging
import os
import select
import signal
import time
from typing import Any, Optional, Set

from django.core.management.base import BaseCommand
from django.db import connection

from .exceptions import PgqException, PgqNoDefinedQueue
from .queue import Queue


class Worker(BaseCommand):
    # The queue to process. Subclass and set this.
    queue: Optional[Queue] = None
    logger = logging.getLogger(__name__)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Create self-pipe for signal handling
        self._signal_pipe_r, self._signal_pipe_w = os.pipe()
        # Make write end non-blocking
        flags = fcntl.fcntl(self._signal_pipe_w, fcntl.F_GETFL, 0)
        fcntl.fcntl(self._signal_pipe_w, fcntl.F_SETFL, flags | os.O_NONBLOCK)

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument(
            "--delay",
            type=float,
            default=0.1,
            help="The number of seconds to wait to check for new tasks.",
        )
        parser.add_argument(
            "--listen",
            action="store_true",
            help="Use LISTEN/NOTIFY to wait for events.",
        )

    def handle_shutdown(self, sig: Any, frame: Any) -> None:
        if self._in_task:
            self.logger.info("Waiting for active tasks to finish...")
            self._shutdown = True
        else:
            self._shutdown = True

        # Write a byte to the pipe to wake up select()
        try:
            os.write(self._signal_pipe_w, b"x")
        except (OSError, IOError):
            # Pipe might be full or closed, that's ok
            pass

    def run_available_tasks(self) -> None:
        """
        Runs tasks continuously until there are no more available.
        """
        # Prevents tasks that failed from blocking others.
        failed_tasks: Set[int] = set()

        if self.queue is None:
            raise PgqNoDefinedQueue

        while True:
            try:
                job = self.queue.run_once(exclude_ids=failed_tasks)
                if job is None:
                    # No more jobs
                    return

                # Only set _in_task = True when we actually have a job to process
                self._in_task = True

            except PgqException as e:
                if e.job is not None:
                    # Make sure we do at least one more iteration of the loop
                    # with the failed task excluded.
                    failed_job = e.job
                    self.logger.exception(
                        "Error in %r: %r.",
                        failed_job,
                        e,
                        extra={"data": {"job": failed_job.to_json()}},
                    )
                    failed_tasks.add(failed_job.id)
                else:
                    raise
            finally:
                # Always clear _in_task flag after processing (or attempting to process)
                self._in_task = False
            if self._shutdown:
                raise InterruptedError

    def handle(self, **options: Any) -> None:  # type: ignore
        self._shutdown = False
        self._in_task = False

        self.delay: float = options["delay"]
        self.listen: bool = options["listen"]

        if self.queue is None:
            raise PgqNoDefinedQueue

        with connection.cursor() as cursor:
            cursor.execute("SET application_name TO %s", ["pgq#{}".format(os.getpid())])

        if self.listen:
            self.queue.listen()
        try:
            # Handle the signals for warm shutdown.
            signal.signal(signal.SIGINT, self.handle_shutdown)
            signal.signal(signal.SIGTERM, self.handle_shutdown)

            while True:
                self.run_available_tasks()
                self.wait()
        except InterruptedError:
            # got shutdown signal
            pass

    def wait(self) -> int:
        if self.listen and self.queue is not None:
            # Use select with both database connection and signal pipe
            readable, _, _ = select.select(
                [connection.connection, self._signal_pipe_r], [], [], self.delay
            )

            # Check if we got a signal
            if self._signal_pipe_r in readable:
                # Clear the pipe
                try:
                    os.read(self._signal_pipe_r, 1024)
                except (OSError, IOError):
                    pass

                # Check if we should shut down
                if self._shutdown and not self._in_task:
                    raise InterruptedError
                return 0

            # Check for database notifications
            if connection.connection in readable:
                connection.connection.poll()
                notifies = self.queue.filter_notifies()
                count = len(notifies)
                self.logger.debug("Woke up with %s NOTIFYs.", count)
                return count

            # Timeout occurred
            return 0
        else:
            # When not using LISTEN, use select on just the signal pipe
            readable, _, _ = select.select([self._signal_pipe_r], [], [], self.delay)

            if self._signal_pipe_r in readable:
                # Clear the pipe
                try:
                    os.read(self._signal_pipe_r, 1024)
                except (OSError, IOError):
                    pass

                # Check if we should shut down
                if self._shutdown and not self._in_task:
                    raise InterruptedError
                return 0

            # Timeout - normal wake up to check for tasks
            return 1
