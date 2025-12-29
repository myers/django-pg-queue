"""
Tests for django.tasks compatible backend.

These tests verify that the PostgresQueueBackend works correctly with
the django.tasks API, including task definition, enqueueing, and status
tracking.
"""

from datetime import timedelta
from typing import Any, Dict

from django.test import TestCase, TransactionTestCase
from django.utils import timezone

from pgq.backend import (
    PostgresQueueBackend,
    Task,
    TaskResult,
    ResultStatus,
    InvalidTask,
    TaskResultDoesNotExist,
    task,
    configure_backend,
)
from pgq.models import Job, TaskResultStatus


# Test task functions
def simple_task(x: int, y: int) -> int:
    """A simple task that adds two numbers."""
    return x + y


def failing_task() -> None:
    """A task that always fails."""
    raise ValueError("This task intentionally fails")


def slow_task(duration: float) -> str:
    """A task that simulates work."""
    import time
    time.sleep(duration)
    return "completed"


# Django tasks style decorated tasks
@task(priority=10, queue_name="high_priority")
def decorated_task(message: str) -> str:
    """A task defined with the django.tasks decorator."""
    return f"processed: {message}"


@task(priority=-5)
def low_priority_task(data: Dict[str, Any]) -> Dict[str, Any]:
    """A low priority task."""
    return {"processed": True, **data}


class PostgresQueueBackendTests(TestCase):
    """Tests for the PostgresQueueBackend class."""

    def setUp(self):
        self.backend = PostgresQueueBackend(
            alias="test",
            options={
                "queue": "test_queue",
                "at_least_once": True,
            }
        )
        configure_backend("test", self.backend)

    def tearDown(self):
        Job.objects.all().delete()

    def test_enqueue_creates_job(self):
        """Enqueueing a task creates a Job in the database."""
        task_obj = Task(
            func=simple_task,
            priority=5,
            queue_name="test_queue",
            backend="test",
        )

        result = self.backend.enqueue(task_obj, args=(1, 2), kwargs={})

        self.assertIsInstance(result, TaskResult)
        self.assertEqual(result.status, ResultStatus.READY)
        self.assertEqual(Job.objects.count(), 1)

        job = Job.objects.first()
        self.assertEqual(job.args, [1, 2])
        self.assertEqual(job.kwargs, {})
        self.assertEqual(job.priority, 5)
        self.assertEqual(job.queue, "test_queue")
        self.assertEqual(job.status, TaskResultStatus.READY)

    def test_enqueue_with_kwargs(self):
        """Enqueueing with keyword arguments stores them correctly."""
        task_obj = Task(
            func=simple_task,
            priority=0,
            queue_name="test_queue",
            backend="test",
        )

        result = self.backend.enqueue(task_obj, args=(), kwargs={"x": 10, "y": 20})

        job = Job.objects.first()
        self.assertEqual(job.args, [])
        self.assertEqual(job.kwargs, {"x": 10, "y": 20})

    def test_enqueue_with_run_after_timedelta(self):
        """Enqueueing with run_after timedelta schedules correctly."""
        delay = timedelta(hours=1)
        task_obj = Task(
            func=simple_task,
            priority=0,
            queue_name="test_queue",
            backend="test",
            run_after=delay,
        )

        before = timezone.now()
        result = self.backend.enqueue(task_obj, args=(1, 2))
        after = timezone.now()

        job = Job.objects.first()
        self.assertGreaterEqual(job.execute_at, before + delay)
        self.assertLessEqual(job.execute_at, after + delay)

    def test_enqueue_with_run_after_datetime(self):
        """Enqueueing with run_after datetime schedules correctly."""
        execute_at = timezone.now() + timedelta(days=1)
        task_obj = Task(
            func=simple_task,
            priority=0,
            queue_name="test_queue",
            backend="test",
            run_after=execute_at,
        )

        result = self.backend.enqueue(task_obj, args=(1, 2))

        job = Job.objects.first()
        self.assertEqual(job.execute_at, execute_at)

    def test_get_result(self):
        """get_result retrieves a TaskResult by ID."""
        task_obj = Task(func=simple_task, priority=0)
        result = self.backend.enqueue(task_obj, args=(1, 2))

        retrieved = self.backend.get_result(result.id)

        self.assertEqual(retrieved.id, result.id)
        self.assertEqual(retrieved.status, ResultStatus.READY)

    def test_get_result_not_found(self):
        """get_result raises TaskResultDoesNotExist for unknown ID."""
        with self.assertRaises(TaskResultDoesNotExist):
            self.backend.get_result("99999")

    def test_validate_task_async_rejected(self):
        """Async tasks are rejected."""
        async def async_task():
            pass

        task_obj = Task(func=async_task, priority=0)

        with self.assertRaises(InvalidTask) as ctx:
            self.backend.validate_task(task_obj)

        self.assertIn("async", str(ctx.exception).lower())

    def test_validate_task_priority_range(self):
        """Priority must be between -100 and 100."""
        task_obj = Task(func=simple_task, priority=101)
        with self.assertRaises(InvalidTask):
            self.backend.validate_task(task_obj)

        task_obj = Task(func=simple_task, priority=-101)
        with self.assertRaises(InvalidTask):
            self.backend.validate_task(task_obj)

        # Valid priorities should pass
        task_obj = Task(func=simple_task, priority=100)
        self.backend.validate_task(task_obj)  # Should not raise

        task_obj = Task(func=simple_task, priority=-100)
        self.backend.validate_task(task_obj)  # Should not raise


class TaskDecoratorTests(TestCase):
    """Tests for the @task decorator."""

    def setUp(self):
        self.backend = PostgresQueueBackend(
            alias="default",
            options={"queue": "default"}
        )
        configure_backend("default", self.backend)

    def tearDown(self):
        Job.objects.all().delete()

    def test_task_decorator_creates_task(self):
        """@task decorator creates a Task object."""
        self.assertIsInstance(decorated_task, Task)
        self.assertEqual(decorated_task.priority, 10)
        self.assertEqual(decorated_task.queue_name, "high_priority")

    def test_task_enqueue_method(self):
        """Task.enqueue() creates a job and returns TaskResult."""
        result = decorated_task.enqueue("hello world")

        self.assertIsInstance(result, TaskResult)
        self.assertEqual(result.status, ResultStatus.READY)
        self.assertEqual(Job.objects.count(), 1)

    def test_task_direct_call(self):
        """Calling task directly executes synchronously."""
        result = decorated_task("test message")

        self.assertEqual(result, "processed: test message")
        # No job created for direct call
        self.assertEqual(Job.objects.count(), 0)

    def test_task_using_modifies_options(self):
        """Task.using() returns new Task with modified options."""
        modified = decorated_task.using(priority=50, queue_name="urgent")

        self.assertEqual(modified.priority, 50)
        self.assertEqual(modified.queue_name, "urgent")
        # Original unchanged
        self.assertEqual(decorated_task.priority, 10)
        self.assertEqual(decorated_task.queue_name, "high_priority")


class JobClaimTests(TransactionTestCase):
    """Tests for the Job.claim() method."""

    def tearDown(self):
        Job.objects.all().delete()

    def test_claim_updates_status(self):
        """Claiming a job updates its status to RUNNING."""
        job = Job.objects.create(
            task="test_task",
            args=[],
            kwargs={},
            status=TaskResultStatus.READY,
        )

        claimed = Job.claim(queue="default", worker_id="worker-1")

        self.assertIsNotNone(claimed)
        self.assertEqual(claimed.id, job.id)
        self.assertEqual(claimed.status, TaskResultStatus.RUNNING)
        self.assertIsNotNone(claimed.started_at)
        self.assertEqual(claimed.attempts, 1)
        self.assertEqual(claimed.worker_id, "worker-1")

    def test_claim_skips_non_ready_jobs(self):
        """Claiming skips jobs that are not READY."""
        Job.objects.create(
            task="running_task",
            args=[],
            kwargs={},
            status=TaskResultStatus.RUNNING,
        )
        Job.objects.create(
            task="failed_task",
            args=[],
            kwargs={},
            status=TaskResultStatus.FAILED,
        )
        Job.objects.create(
            task="successful_task",
            args=[],
            kwargs={},
            status=TaskResultStatus.SUCCESSFUL,
        )

        claimed = Job.claim(queue="default")

        self.assertIsNone(claimed)

    def test_claim_respects_execute_at(self):
        """Claiming skips jobs with future execute_at."""
        future = timezone.now() + timedelta(hours=1)
        Job.objects.create(
            task="future_task",
            args=[],
            kwargs={},
            status=TaskResultStatus.READY,
            execute_at=future,
        )

        claimed = Job.claim(queue="default")

        self.assertIsNone(claimed)

    def test_claim_respects_priority(self):
        """Higher priority jobs are claimed first."""
        low = Job.objects.create(
            task="low_priority",
            args=[],
            kwargs={},
            priority=-10,
            status=TaskResultStatus.READY,
        )
        high = Job.objects.create(
            task="high_priority",
            args=[],
            kwargs={},
            priority=10,
            status=TaskResultStatus.READY,
        )

        claimed = Job.claim(queue="default")

        self.assertEqual(claimed.id, high.id)

    def test_mark_successful(self):
        """mark_successful updates job status and stores return value."""
        job = Job.objects.create(
            task="test_task",
            args=[],
            kwargs={},
            status=TaskResultStatus.RUNNING,
        )

        job.mark_successful(return_value={"result": 42})

        job.refresh_from_db()
        self.assertEqual(job.status, TaskResultStatus.SUCCESSFUL)
        self.assertIsNotNone(job.finished_at)
        self.assertEqual(job.return_value, {"result": 42})

    def test_mark_failed(self):
        """mark_failed updates job status and stores error info."""
        job = Job.objects.create(
            task="test_task",
            args=[],
            kwargs={},
            status=TaskResultStatus.RUNNING,
        )

        job.mark_failed(
            error_traceback="Traceback...",
            error_class="ValueError"
        )

        job.refresh_from_db()
        self.assertEqual(job.status, TaskResultStatus.FAILED)
        self.assertIsNotNone(job.finished_at)
        self.assertEqual(job.error_traceback, "Traceback...")
        self.assertEqual(job.error_class, "ValueError")

    def test_reset_for_retry(self):
        """reset_for_retry resets job to READY status."""
        job = Job.objects.create(
            task="test_task",
            args=[],
            kwargs={},
            status=TaskResultStatus.RUNNING,
            started_at=timezone.now(),
            worker_id="worker-1",
        )

        job.reset_for_retry()

        job.refresh_from_db()
        self.assertEqual(job.status, TaskResultStatus.READY)
        self.assertIsNone(job.started_at)
        self.assertIsNone(job.worker_id)


class BackendExecutionTests(TransactionTestCase):
    """Tests for task execution via the backend."""

    def setUp(self):
        self.backend = PostgresQueueBackend(
            alias="test",
            options={
                "queue": "test_queue",
                "at_least_once": True,
            }
        )
        configure_backend("test", self.backend)

    def tearDown(self):
        Job.objects.all().delete()

    def test_execute_job_success(self):
        """Successful job execution updates status correctly."""
        task_obj = Task(func=simple_task, priority=0, backend="test")
        result = self.backend.enqueue(task_obj, args=(3, 4))

        job = Job.objects.get(id=result.id)
        job.status = TaskResultStatus.RUNNING
        job.save()

        return_value = self.backend.execute_job(job)

        self.assertEqual(return_value, 7)
        job.refresh_from_db()
        self.assertEqual(job.status, TaskResultStatus.SUCCESSFUL)
        self.assertEqual(job.return_value, 7)

    def test_execute_job_failure(self):
        """Failed job execution updates status with error info."""
        task_obj = Task(func=failing_task, priority=0, backend="test")
        result = self.backend.enqueue(task_obj, args=())

        job = Job.objects.get(id=result.id)
        job.status = TaskResultStatus.RUNNING
        job.save()

        with self.assertRaises(ValueError):
            self.backend.execute_job(job)

        job.refresh_from_db()
        self.assertEqual(job.status, TaskResultStatus.FAILED)
        self.assertEqual(job.error_class, "ValueError")
        self.assertIn("intentionally fails", job.error_traceback)

    def test_run_once_processes_job(self):
        """run_once claims and executes a job."""
        task_obj = Task(func=simple_task, priority=0, backend="test")
        self.backend.enqueue(task_obj, args=(5, 6))

        result = self.backend.run_once(worker_id="test-worker")

        self.assertIsNotNone(result)
        self.assertEqual(result.status, ResultStatus.SUCCESSFUL)

        job = Job.objects.first()
        self.assertEqual(job.status, TaskResultStatus.SUCCESSFUL)
        self.assertEqual(job.return_value, 11)

    def test_run_once_returns_none_when_empty(self):
        """run_once returns None when no jobs available."""
        result = self.backend.run_once()

        self.assertIsNone(result)


class TaskResultTests(TestCase):
    """Tests for TaskResult properties."""

    def test_is_finished(self):
        """is_finished returns True for terminal states."""
        successful = TaskResult(
            id="1", task=None, status=ResultStatus.SUCCESSFUL,
            enqueued_at=timezone.now()
        )
        failed = TaskResult(
            id="2", task=None, status=ResultStatus.FAILED,
            enqueued_at=timezone.now()
        )
        ready = TaskResult(
            id="3", task=None, status=ResultStatus.READY,
            enqueued_at=timezone.now()
        )
        running = TaskResult(
            id="4", task=None, status=ResultStatus.RUNNING,
            enqueued_at=timezone.now()
        )

        self.assertTrue(successful.is_finished)
        self.assertTrue(failed.is_finished)
        self.assertFalse(ready.is_finished)
        self.assertFalse(running.is_finished)

    def test_is_successful(self):
        """is_successful returns True only for SUCCESSFUL status."""
        successful = TaskResult(
            id="1", task=None, status=ResultStatus.SUCCESSFUL,
            enqueued_at=timezone.now()
        )
        failed = TaskResult(
            id="2", task=None, status=ResultStatus.FAILED,
            enqueued_at=timezone.now()
        )

        self.assertTrue(successful.is_successful)
        self.assertFalse(failed.is_successful)

    def test_is_failed(self):
        """is_failed returns True only for FAILED status."""
        successful = TaskResult(
            id="1", task=None, status=ResultStatus.SUCCESSFUL,
            enqueued_at=timezone.now()
        )
        failed = TaskResult(
            id="2", task=None, status=ResultStatus.FAILED,
            enqueued_at=timezone.now()
        )

        self.assertFalse(successful.is_failed)
        self.assertTrue(failed.is_failed)


class JobToJsonTests(TestCase):
    """Tests for Job.to_json() with new fields."""

    def test_to_json_includes_new_fields(self):
        """to_json() includes all django.tasks fields."""
        job = Job.objects.create(
            task="test_task",
            args=[1, 2, 3],
            kwargs={"key": "value"},
            status=TaskResultStatus.RUNNING,
            priority=10,
            queue="test",
            attempts=2,
            worker_id="worker-1",
        )

        json_data = job.to_json()

        self.assertEqual(json_data["args"], [1, 2, 3])
        self.assertEqual(json_data["kwargs"], {"key": "value"})
        self.assertEqual(json_data["status"], TaskResultStatus.RUNNING)
        self.assertEqual(json_data["attempts"], 2)
        self.assertEqual(json_data["worker_id"], "worker-1")
        self.assertIn("started_at", json_data)
        self.assertIn("finished_at", json_data)
        self.assertIn("return_value", json_data)
        self.assertIn("error_traceback", json_data)
        self.assertIn("error_class", json_data)
