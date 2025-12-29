# Generated migration for django.tasks compatibility
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("pgq", "0001_initial"),
    ]

    operations = [
        # Add kwargs field for django.tasks style argument passing
        migrations.AddField(
            model_name="job",
            name="kwargs",
            field=models.JSONField(
                default=dict,
                help_text="Keyword arguments for django.tasks style tasks.",
            ),
        ),
        # Add status field for task state tracking
        migrations.AddField(
            model_name="job",
            name="status",
            field=models.CharField(
                choices=[
                    ("READY", "Ready"),
                    ("RUNNING", "Running"),
                    ("SUCCESSFUL", "Successful"),
                    ("FAILED", "Failed"),
                ],
                db_index=True,
                default="READY",
                help_text="Current status of the task.",
                max_length=20,
            ),
        ),
        # Add started_at field for tracking when task execution began
        migrations.AddField(
            model_name="job",
            name="started_at",
            field=models.DateTimeField(
                blank=True,
                help_text="When task execution started.",
                null=True,
            ),
        ),
        # Add finished_at field for tracking when task completed
        migrations.AddField(
            model_name="job",
            name="finished_at",
            field=models.DateTimeField(
                blank=True,
                help_text="When task execution completed.",
                null=True,
            ),
        ),
        # Add return_value field for storing task results
        migrations.AddField(
            model_name="job",
            name="return_value",
            field=models.JSONField(
                blank=True,
                help_text="Return value from successful task execution.",
                null=True,
            ),
        ),
        # Add error_traceback field for storing failure information
        migrations.AddField(
            model_name="job",
            name="error_traceback",
            field=models.TextField(
                blank=True,
                help_text="Full traceback if task failed.",
                null=True,
            ),
        ),
        # Add error_class field for storing exception type
        migrations.AddField(
            model_name="job",
            name="error_class",
            field=models.CharField(
                blank=True,
                help_text="Exception class name if task failed.",
                max_length=255,
                null=True,
            ),
        ),
        # Add attempts field for tracking retry count
        migrations.AddField(
            model_name="job",
            name="attempts",
            field=models.IntegerField(
                default=0,
                help_text="Number of execution attempts.",
            ),
        ),
        # Add worker_id field for tracking which worker is processing
        migrations.AddField(
            model_name="job",
            name="worker_id",
            field=models.CharField(
                blank=True,
                help_text="Identifier of the worker processing this task.",
                max_length=255,
                null=True,
            ),
        ),
        # Add composite index for status + queue queries
        migrations.AddIndex(
            model_name="job",
            index=models.Index(
                fields=["status", "queue"],
                name="pgq_job_status_queue_idx",
            ),
        ),
        # Set default for args field (was previously not nullable)
        migrations.AlterField(
            model_name="job",
            name="args",
            field=models.JSONField(default=dict),
        ),
    ]
