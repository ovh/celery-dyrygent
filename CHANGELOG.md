v0.9.0
--------
Added Python 3.10 support
Remove Python 3.6 support due to Python 3.6 EOL

Added celery5 support
Drop celery3 support

Tests: Integration tests for every supported Python version
Tests: Integration tests for different celery versions
Tests: Integration tests for two backends - redis and sqlalchemy(mysql)

v0.8.0
------
Feature: Celery options can be set for workflow processor and workflow tasks
Feature: Added ```custom_payload``` field for entire workflow and tasks

v0.7.0
------
Added python3 support
