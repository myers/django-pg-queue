"""
Management command for running the django.tasks compatible worker.

Usage:
    python manage.py pgq_worker
    python manage.py pgq_worker --backend myqueue
    python manage.py pgq_worker --listen
    python manage.py pgq_worker --delay 5
"""

from pgq.tasks_worker import DjangoTasksWorker


class Command(DjangoTasksWorker):
    """Run a worker to process background tasks using the PostgreSQL queue backend."""
    pass
