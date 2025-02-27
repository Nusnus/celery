import time

import pytest

from celery.worker import state


class test_revoked:
    """Test suite for the state.revoked global."""

    def setup_method(self):
        state.revoked.clear()

    def teardown_method(self):
        state.revoked.clear()

    @pytest.mark.parametrize('task_id', [
        'simple-id',
        '123',
        'id-with-dashes',
        'id.with.dots',
    ])
    def test_add_and_membership(self, task_id):
        """Test adding a task ID and checking membership."""
        state.revoked.add(task_id)
        assert task_id in state.revoked

    @pytest.mark.parametrize('task_ids', [
        ['id1', 'id2', 'id3'],
        ['task-1', 'task-2'],
        ['single'],
        [],  # Empty list
    ])
    def test_update_with_iterable(self, task_ids):
        """Test updating the revoked set with an iterable."""
        state.revoked.update(task_ids)
        for task_id in task_ids:
            assert task_id in state.revoked

    @pytest.mark.parametrize('task_id', [
        'id1',
        'non-existent',  # Should not raise error
    ])
    def test_pop_value(self, task_id):
        """Test removing items with pop_value method."""
        if task_id != 'non-existent':
            state.revoked.add(task_id)
        state.revoked.pop_value(task_id)
        assert task_id not in state.revoked

    @pytest.mark.parametrize('task_id', [
        'id1',
        'non-existent',  # Should not raise error
    ])
    def test_discard(self, task_id):
        """Test discarding items from the revoked set."""
        if task_id != 'non-existent':
            state.revoked.add(task_id)
        state.revoked.discard(task_id)
        assert task_id not in state.revoked

    def test_clear(self):
        """Test clearing the revoked set."""
        task_ids = ['id1', 'id2', 'id3']
        state.revoked.update(task_ids)
        state.revoked.clear()
        assert len(state.revoked) == 0

    def test_purge_removes_expired_items(self):
        """Test that purge removes expired items."""
        original_expires = state.revoked.expires
        try:
            state.revoked.expires = 10  # 10 seconds

            past_time = time.monotonic() - 20
            state.revoked.add('expired_id', now=past_time)

            current_time = time.monotonic()
            state.revoked.add('recent_id', now=current_time)

            assert 'expired_id' in state.revoked, "Expired item should be present before purge"
            assert 'recent_id' in state.revoked, "Recent item should be present before purge"

            state.revoked.purge(now=current_time)

            assert 'expired_id' not in state.revoked, "Expired item should be removed after purge"
            assert 'recent_id' in state.revoked, "Recent item should remain after purge"
        finally:
            state.revoked.expires = original_expires

    @pytest.mark.parametrize('value, expected', [
        (123, True),       # Integer
        ('123', False),    # String that looks like an integer
        (None, False),     # None
    ])
    def test_type_sensitivity(self, value, expected):
        """Test that membership check is type-sensitive."""
        state.revoked.add(123)
        result = value in state.revoked
        assert result == expected
