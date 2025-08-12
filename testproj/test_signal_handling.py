"""Tests for improved signal handling with self-pipe pattern."""

import os
import signal
import threading
import time
import unittest
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from django.test import TestCase

from pgq.commands import Worker
from pgq.queue import Queue


class MockQueue(Queue):
    """Mock queue for testing."""

    def __init__(self):
        super().__init__(tasks={})
        self.wait_called = False
        self.filter_notifies_called = False

    def wait(self, timeout):
        """Mock wait that simulates blocking."""
        self.wait_called = True
        time.sleep(timeout)
        return []

    def filter_notifies(self):
        """Mock filter_notifies."""
        self.filter_notifies_called = True
        return []

    def run_once(self, exclude_ids=None):
        """Return None to simulate no jobs."""
        return None


class SignalHandlingTests(TestCase):
    """Test the self-pipe signal handling implementation."""

    # Test timing constants - can be overridden by environment variables
    SIGNAL_DELAY = float(os.environ.get("SIGNAL_TEST_DELAY", "0.1"))
    MAX_INTERRUPT_TIME = float(os.environ.get("SIGNAL_TEST_MAX_INTERRUPT", "0.3"))
    MIN_TIMEOUT_TIME = float(os.environ.get("SIGNAL_TEST_MIN_TIMEOUT", "0.09"))
    WORKER_DELAY = float(os.environ.get("SIGNAL_TEST_WORKER_DELAY", "0.5"))
    SHORT_TIMEOUT = float(os.environ.get("SIGNAL_TEST_SHORT_TIMEOUT", "0.1"))

    def setUp(self):
        """Set up test fixtures."""
        self.worker = Worker()
        self.worker.queue = MockQueue()
        self.worker._shutdown = False
        self.worker._in_task = False
        self.worker.delay = self.WORKER_DELAY
        self.worker.listen = False

    def tearDown(self):
        """Clean up after tests."""
        self._cleanup_worker_pipes()

    def _cleanup_worker_pipes(self):
        """Helper method to clean up worker pipes."""
        if hasattr(self.worker, "_signal_pipe_r"):
            try:
                os.close(self.worker._signal_pipe_r)
                os.close(self.worker._signal_pipe_w)
            except (OSError, IOError):
                pass

    @contextmanager
    def pipe_cleanup(self, pipe_r, pipe_w):
        """Context manager for automatic pipe cleanup."""
        try:
            yield pipe_r, pipe_w
        finally:
            try:
                os.close(pipe_r)
                os.close(pipe_w)
            except (OSError, IOError):
                pass

    def test_self_pipe_creation(self):
        """Test that self-pipe is created on initialization."""
        self.assertTrue(hasattr(self.worker, "_signal_pipe_r"))
        self.assertTrue(hasattr(self.worker, "_signal_pipe_w"))

        # Verify pipes are valid file descriptors
        self.assertIsInstance(self.worker._signal_pipe_r, int)
        self.assertIsInstance(self.worker._signal_pipe_w, int)
        self.assertGreater(self.worker._signal_pipe_r, 0)
        self.assertGreater(self.worker._signal_pipe_w, 0)

    def test_signal_handler_writes_to_pipe(self):
        """Test that signal handler writes to the pipe."""
        # Call the signal handler
        self.worker.handle_shutdown(signal.SIGINT, None)

        # Check that shutdown flag is set
        self.assertTrue(self.worker._shutdown)

        # Verify data was written to pipe
        import select

        readable, _, _ = select.select([self.worker._signal_pipe_r], [], [], 0)
        self.assertIn(self.worker._signal_pipe_r, readable)

        # Read and verify the data
        data = os.read(self.worker._signal_pipe_r, 1)
        self.assertEqual(data, b"x")

    def test_wait_interrupted_by_signal(self):
        """Test that wait() is interrupted by signal via pipe."""
        # Track how long wait takes
        start_time = time.time()

        # Set up a thread to send signal after short delay
        def send_signal_after_delay():
            time.sleep(self.SIGNAL_DELAY)
            self.worker.handle_shutdown(signal.SIGINT, None)

        signal_thread = threading.Thread(target=send_signal_after_delay)
        signal_thread.start()

        # Call wait with a long timeout
        with self.assertRaises(InterruptedError):
            self.worker.wait()

        elapsed = time.time() - start_time
        signal_thread.join()

        # Should have been interrupted quickly, not waited full delay
        self.assertLess(elapsed, self.MAX_INTERRUPT_TIME)
        self.assertTrue(self.worker._shutdown)

    @patch("django.db.connection")
    def test_wait_with_listen_mode_and_signal(self, mock_connection):
        """Test wait() with LISTEN mode handles signals properly."""
        self.worker.listen = True
        mock_connection.connection = MagicMock()

        # Mock select to simulate signal pipe being readable
        with patch("select.select") as mock_select:
            # Simulate signal pipe being readable
            mock_select.return_value = ([self.worker._signal_pipe_r], [], [])

            # Write to the pipe to simulate signal
            os.write(self.worker._signal_pipe_w, b"x")
            self.worker._shutdown = True

            # Should raise InterruptedError
            with self.assertRaises(InterruptedError):
                self.worker.wait()

    def test_wait_with_listen_mode_database_notify(self):
        """Test wait() with LISTEN mode handles database notifications."""
        self.worker.listen = True

        # Mock the connection in the commands module
        with patch("pgq.commands.connection") as mock_connection:
            mock_db_connection = MagicMock()
            mock_connection.connection = mock_db_connection

            # Mock select to simulate database connection being readable
            with patch("select.select") as mock_select:
                mock_select.return_value = ([mock_db_connection], [], [])

                # Should process database notifications
                result = self.worker.wait()

                # Verify poll was called
                mock_db_connection.poll.assert_called_once()
                # Verify filter_notifies was called
                self.assertTrue(self.worker.queue.filter_notifies_called)
                self.assertEqual(result, 0)  # Empty list of notifications

    def test_wait_timeout_without_signal(self):
        """Test that wait() times out normally when no signal."""
        self.worker.delay = self.SHORT_TIMEOUT
        start_time = time.time()

        # Should return normally after timeout
        result = self.worker.wait()

        elapsed = time.time() - start_time
        # Should have waited approximately the full delay
        self.assertGreaterEqual(elapsed, self.MIN_TIMEOUT_TIME)
        self.assertEqual(result, 1)  # Normal timeout return

    def test_cleanup_on_shutdown(self):
        """Test that pipes are cleaned up properly."""
        # Store the file descriptors
        pipe_r = self.worker._signal_pipe_r
        pipe_w = self.worker._signal_pipe_w

        # Use context manager for proper cleanup testing
        with self.pipe_cleanup(pipe_r, pipe_w):
            # Pipes should be valid before cleanup
            self.assertIsInstance(pipe_r, int)
            self.assertIsInstance(pipe_w, int)

        # Verify pipes are closed after context manager exits
        with self.assertRaises(OSError):
            os.read(pipe_r, 1)

        with self.assertRaises(OSError):
            os.write(pipe_w, b"x")

    def test_signal_during_task_processing(self):
        """Test that signal during task processing sets flag but doesn't interrupt."""
        self.worker._in_task = True

        # Send signal while "in task"
        self.worker.handle_shutdown(signal.SIGINT, None)

        # Should set shutdown flag
        self.assertTrue(self.worker._shutdown)

        # Should have written to pipe
        import select

        readable, _, _ = select.select([self.worker._signal_pipe_r], [], [], 0)
        self.assertIn(self.worker._signal_pipe_r, readable)

        # But wait should not raise InterruptedError when in_task is True
        self.worker._in_task = True  # Ensure it's still True
        result = self.worker.wait()
        self.assertEqual(result, 0)  # Should return normally


if __name__ == "__main__":
    unittest.main()
