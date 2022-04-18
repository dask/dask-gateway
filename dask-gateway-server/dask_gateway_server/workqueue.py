import asyncio
import collections

__all__ = ("Backoff", "WorkQueue", "WorkQueueClosed")


class Backoff:
    """Manages backoffs for items in the work queue.

    An items backoff is calculated as:

    ```
    min(base_delay * 2**n_failures, max_delay)
    ```
    """

    def __init__(self, base_delay=0.005, max_delay=300):
        self._failures = collections.defaultdict(int)
        self.base_delay = base_delay
        self.max_delay = max_delay

    def backoff(self, item):
        """Get the backoff time for an item (in seconds)"""
        exp = self._failures[item]
        self._failures[item] = exp + 1
        backoff = min(self.base_delay * 2**exp, self.max_delay)
        return backoff

    def failures(self, item):
        """Get the number of failures seen for an item"""
        return self._failures.get(item, 0)

    def reset(self, item):
        """Reset the backoff for an item"""
        self._failures.pop(item, None)


class WorkQueueClosed(Exception):
    """Indicates that the WorkQueue is closed."""

    pass


class WorkQueue:
    """An asynchronous work queue.

    This work queue is designed with the following design:
    - Supports multiple asynchronous producers/consumers
    - If a task is placed in the queue multiple times before being consumed,
      it will only be consumed once (duplicate puts are merged).
    - If a task is currently being processed, no other consumer may process
      this task (even if it's been re-added to the queue) until the task is
      marked as done using ``task_done``.
    - Tasks can be added after a delay with ``put_after``
    - Tasks can be requeued with backoff after a failure with ``put_backoff``.
      On success the backoff can be reset with ``reset_backoff``.
    """

    def __init__(self, backoff=None):
        self.backoff = backoff or Backoff()
        self._loop = asyncio.get_event_loop()
        self._waiting = collections.deque()
        self._queue = collections.deque()
        self._processing = set()
        self._dirty = set()
        self._timers = {}
        self.closed = False

    def is_empty(self):
        """True if there are no items queued"""
        return not self._dirty

    def _put(self, item):
        if item not in self._dirty:
            self._dirty.add(item)
            if item not in self._processing:
                self._queue.append(item)

    def _get(self):
        if self.closed:
            raise WorkQueueClosed
        item = self._queue.popleft()
        self._processing.add(item)
        self._dirty.discard(item)
        return item

    def _wakeup_next(self):
        while self._waiting:
            waiter = self._waiting.popleft()
            if not waiter.done():
                waiter.set_result(None)
                break

    def _wakeup_all(self):
        while self._waiting:
            waiter = self._waiting.popleft()
            if not waiter.done():
                waiter.set_result(None)

    def _put_delayed(self, item):
        self._timers.pop(item, None)
        self.put(item)

    def put(self, item):
        """Put an item in the queue"""
        self._put(item)
        self._wakeup_next()

    def put_after(self, item, delay):
        """Schedule an item to be put in the queue after a delay.

        If the item is already scheduled, it will be rescheduled only if the
        delay would enqueue it sooner than the existing schedule.
        """
        when = self._loop.time() + delay
        existing = self._timers.get(item, None)
        # Schedule if new or if sooner than existing schedule
        if existing is None or existing[1] > when:
            if existing is not None:
                existing[0].cancel()

            if delay > 0:
                self._timers[item] = (
                    self._loop.call_at(when, self._put_delayed, item),
                    when,
                )
            else:
                self._timers.pop(item, None)
                self.put(item)

    def put_backoff(self, item):
        """Schedule an item to be put in the queue after a backoff.

        If the item is already scheduled, it will be rescheduled only if the
        delay would enqueue it sooner than the existing schedule.
        """
        self.put_after(item, self.backoff.backoff(item))

    def failures(self, item):
        """Get the number of failures seen for this item"""
        return self.backoff.failures(item)

    def reset_backoff(self, item):
        """Reset the backoff for an item"""
        self.backoff.reset(item)

    async def get(self):
        """Get an item from the queue."""
        while not self._queue:
            if self.closed:
                raise WorkQueueClosed
            waiter = self._loop.create_future()
            self._waiting.append(waiter)
            try:
                await waiter
            except asyncio.CancelledError:
                try:
                    self._waiting.remove(waiter)
                except ValueError:
                    pass
                raise
        return self._get()

    def task_done(self, item):
        """Mark a task as done.

        This *must* be done before the item can be processed again.
        """
        self._processing.discard(item)
        if item in self._dirty:
            self._queue.append(item)
            self._wakeup_next()

    def close(self):
        """Close the queue.

        Future calls to ``WorkQueue.get`` will raise ``WorkQueueClosed``
        """
        self.closed = True
        self._wakeup_all()
