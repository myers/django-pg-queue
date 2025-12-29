django-pg-queue
===============

django-pg-queue is a task queue system for Django 6.0+ backed by PostgreSQL,
implementing the ``django.tasks`` API.

It was forked from django-postgres-queue (https://github.com/gavinwahl/django-postgres-queue/)
written by Gavin Wahl, and has been updated to support Django's new task backend interface.


Why PostgreSQL?
---------------

PostgreSQL has features that make it an excellent task queue backend:

- **Transactional behavior and reliability.**

  Adding tasks is atomic with respect to other database work. There is no need
  to use ``transaction.on_commit`` hooks and there is no risk of a transaction
  being committed but the tasks it queued being lost.

  Processing tasks is atomic with respect to other database work. Database work
  done by a task will either be committed, or the task will not be marked as
  processed, no exceptions.

- **Operational simplicity**

  By reusing the durable, transactional storage that we're already using
  anyway, there's no need to configure, monitor, and backup another stateful
  service. For small teams and light workloads, this is the right trade-off.

- **Easy introspection**

  Since tasks are stored in a database table, it's easy to query and monitor
  the state of the queue with SQL.

- **Safety**

  By using PostgreSQL transactions, there is no possibility of jobs being left
  in a locked or ambiguous state if a worker dies. Tasks immediately become
  available for another worker to pick up.

- **Priority queues**

  Ordering is specified explicitly when selecting the next task to work on,
  making it easy to ensure high-priority tasks are processed first.


How it works
------------

django-pg-queue uses PostgreSQL's ``SELECT FOR UPDATE SKIP LOCKED`` to
efficiently claim tasks without blocking other workers:

.. code:: sql

    UPDATE pgq_job
    SET status = 'RUNNING', started_at = now(), attempts = attempts + 1
    WHERE id = (
        SELECT id
        FROM pgq_job
        WHERE execute_at <= now() AND status = 'READY'
        ORDER BY priority DESC, created_at
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    )
    RETURNING *;

Tasks are tracked with status fields (``READY``, ``RUNNING``, ``SUCCESSFUL``,
``FAILED``) allowing you to monitor progress and retrieve results.


Requirements
------------

- Python 3.10+
- Django 6.0+
- PostgreSQL 12+


Installation
------------

Install with pip::

  pip install django-pg-queue

Add ``'pgq'`` to your ``INSTALLED_APPS`` and run migrations::

  python manage.py migrate pgq


Configuration
-------------

Configure the task backend in your Django settings:

.. code:: python

    TASKS = {
        'default': {
            'BACKEND': 'pgq.backend.PostgresQueueBackend',
            'OPTIONS': {
                'queue': 'default',
                'notify_channel': 'pgq_default',
            }
        }
    }


Defining Tasks
--------------

Use the ``@task`` decorator to define tasks:

.. code:: python

    from pgq import task

    @task(priority=10, queue_name='emails')
    def send_email(recipient, subject, body):
        """Send an email to a recipient."""
        # Your email sending logic here
        return {'sent': True, 'recipient': recipient}

    @task()
    def process_data(data_id):
        """Process some data."""
        data = Data.objects.get(id=data_id)
        # Process the data
        return {'processed': True}


Enqueueing Tasks
----------------

Enqueue a task by calling ``.enqueue()`` on it:

.. code:: python

    # Enqueue with positional arguments
    result = send_email.enqueue('user@example.com', 'Hello', 'World')

    # Enqueue with keyword arguments
    result = send_email.enqueue(
        recipient='user@example.com',
        subject='Hello',
        body='World'
    )

    # The result is a TaskResult object
    print(result.id)      # Unique task ID
    print(result.status)  # 'READY'


Task Options
------------

Tasks support several options:

.. code:: python

    from datetime import timedelta
    from pgq import task

    @task(
        priority=50,           # Higher = processed sooner (-100 to 100)
        queue_name='critical', # Queue name for filtering
        backend='default',     # Backend alias from TASKS setting
    )
    def important_task(data):
        pass

    # Override options when enqueueing
    result = important_task.using(
        priority=100,
        run_after=timedelta(hours=1),  # Delay execution
    ).enqueue(data={'key': 'value'})


Checking Task Results
---------------------

Retrieve task results using the backend:

.. code:: python

    from pgq import get_task_backend

    backend = get_task_backend()

    # Get a result by ID
    result = backend.get_result(result_id)

    print(result.status)        # 'READY', 'RUNNING', 'SUCCESSFUL', or 'FAILED'
    print(result.is_finished)   # True if SUCCESSFUL or FAILED
    print(result.is_successful) # True if SUCCESSFUL
    print(result.return_value)  # Return value from the task (if successful)
    print(result.errors)        # List of errors (if failed)
    print(result.attempts)      # Number of execution attempts


Running Workers
---------------

Start a worker using the management command::

  python manage.py pgq_worker

Options::

  --backend ALIAS    Task backend to use (default: 'default')
  --delay SECONDS    Polling interval in seconds (default: 1)
  --listen           Use PostgreSQL LISTEN/NOTIFY for instant notification
  --worker-id ID     Unique identifier for this worker

For production, use ``--listen`` for efficient notification-based processing::

  python manage.py pgq_worker --listen


Monitoring
----------

Tasks are stored in the ``pgq_job`` table. Monitor with SQL:

.. code:: sql

    -- Count tasks by status
    SELECT status, count(*) FROM pgq_job GROUP BY status;

    -- Count ready tasks by queue
    SELECT queue, count(*) FROM pgq_job
    WHERE status = 'READY' AND execute_at <= now()
    GROUP BY queue;

    -- View failed tasks
    SELECT id, task, error_class, created_at FROM pgq_job
    WHERE status = 'FAILED'
    ORDER BY created_at DESC;

    -- View running tasks
    SELECT id, task, worker_id, started_at, attempts FROM pgq_job
    WHERE status = 'RUNNING';


Migrating from Legacy API (< 0.9.0)
-----------------------------------

Version 0.9.0 introduces the Django 6.0 ``django.tasks`` API and removes the
legacy queue-based API. Here's how to migrate:

**Before (Legacy API):**

.. code:: python

    from pgq.queue import AtLeastOnceQueue
    from pgq.decorators import task

    queue = AtLeastOnceQueue(
        tasks={},
        queue='my-queue',
        notify_channel='my-queue',
    )

    @task(queue)
    def send_email(queue, job):
        recipient = job.args['recipient']
        subject = job.args['subject']
        # ...

    # Enqueueing
    send_email.enqueue({'recipient': 'user@example.com', 'subject': 'Hello'})

**After (New API):**

.. code:: python

    from pgq import task

    @task(queue_name='my-queue')
    def send_email(recipient, subject):
        # Arguments are passed directly
        # ...

    # Enqueueing
    send_email.enqueue(recipient='user@example.com', subject='Hello')

**Key differences:**

1. **No queue instance needed** - Configure backends in Django settings instead
2. **Direct arguments** - Task functions receive arguments directly, not via ``job.args``
3. **No queue/job parameters** - Task signature is just your business logic parameters
4. **TaskResult returned** - ``enqueue()`` returns a ``TaskResult`` with status tracking
5. **New worker command** - Use ``pgq_worker`` instead of custom worker commands

**Settings migration:**

.. code:: python

    # Add to settings.py
    TASKS = {
        'default': {
            'BACKEND': 'pgq.backend.PostgresQueueBackend',
            'OPTIONS': {
                'queue': 'my-queue',
                'notify_channel': 'my-queue',
            }
        }
    }

**Database migration:**

Run migrations to add the new status tracking fields::

  python manage.py migrate pgq


Logging
-------

django-pg-queue logs through Python's logging framework under the ``pgq``
namespace:

.. code:: python

    LOGGING = {
        'version': 1,
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'INFO',
            },
        },
        'loggers': {
            'pgq': {
                'handlers': ['console'],
                'level': 'INFO',
            },
        }
    }


License
-------

BSD License
