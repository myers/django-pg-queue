# Generated by Django 2.0.3 on 2019-04-19 20:57

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('dpq', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='job',
            name='priority',
            field=models.IntegerField(default=0, help_text='Jobs with higher priority will be processed first.'),
        ),
    ]
