"""Timer scheduling, firing, and removal for the current key."""

import asyncio
from datetime import datetime, timedelta, timezone

from prosody import Context, ExciseMessage, EventHandler, Message, Timer
import tsasync

from support import DEFAULT_TIMEOUT



# Timer precision tolerance for tests (in seconds)
TIMER_TOLERANCE_SECONDS = 1

class CallbackTimerHandler(EventHandler):
    """Handler that executes a custom callback function for timer testing"""

    async def on_excise(self, context: Context, message: ExciseMessage) -> None:
        return None

    __test__ = False

    def __init__(self, message_callback=None):
        self.timer_events = tsasync.Channel()
        self.results = tsasync.Channel()
        self.message_callback = message_callback

    async def on_message(self, context: Context, message: Message) -> None:
        if self.message_callback:
            try:
                result = await self.message_callback(context, message)
                if result is not None:
                    await self.results.send({"success": True, **result})
            except Exception as e:
                await self.results.send({"success": False, "error": str(e)})
        else:
            await self.results.send({"context": context, "message": message})

    async def on_timer(self, context: Context, timer: Timer) -> None:
        await self.timer_events.send({
            "context": context,
            "timer": timer
        })

class TimerTestHandler(CallbackTimerHandler):
    """Backwards compatibility wrapper"""
    __test__ = False

    def __init__(self):
        super().__init__()
        self.message_events = self.results
        self.operation_results = self.results

async def test_timer_scheduling_and_firing(client, random_topic_and_group):

    topic, _ = random_topic_and_group

    async def schedule_timer_callback(context: Context, message: Message):
        scheduled_time = datetime.now(timezone.utc) + timedelta(seconds=2)
        await asyncio.wait_for(context.schedule(scheduled_time), timeout=DEFAULT_TIMEOUT)
        return {"scheduled_time": scheduled_time}

    handler = CallbackTimerHandler(schedule_timer_callback)

    await asyncio.wait_for(client.subscribe(handler), timeout=DEFAULT_TIMEOUT)

    test_key = "timer-test-key"
    await asyncio.wait_for(client.send(topic, test_key, {"test": "data"}), timeout=DEFAULT_TIMEOUT)

    operation_result = await asyncio.wait_for(handler.results.receive(), timeout=DEFAULT_TIMEOUT)
    assert operation_result["success"]
    scheduled_time = operation_result["scheduled_time"]

    timer_event = await asyncio.wait_for(handler.timer_events.receive(), timeout=DEFAULT_TIMEOUT)

    assert timer_event["timer"].key == test_key
    time_diff = abs((timer_event["timer"].time - scheduled_time).total_seconds())
    assert time_diff <= TIMER_TOLERANCE_SECONDS

async def test_timer_unschedule(client, random_topic_and_group):

    topic, _ = random_topic_and_group

    async def unschedule_timer_callback(context: Context, message: Message):
        timer1_time = datetime.now(timezone.utc) + timedelta(seconds=3)
        timer2_time = datetime.now(timezone.utc) + timedelta(seconds=4)

        await asyncio.wait_for(context.schedule(timer1_time), timeout=DEFAULT_TIMEOUT)
        await asyncio.wait_for(context.schedule(timer2_time), timeout=DEFAULT_TIMEOUT)

        scheduled = await asyncio.wait_for(context.scheduled(), timeout=DEFAULT_TIMEOUT)
        assert len(scheduled) == 2

        await asyncio.wait_for(context.unschedule(timer1_time), timeout=DEFAULT_TIMEOUT)

        scheduled_after = await asyncio.wait_for(context.scheduled(), timeout=DEFAULT_TIMEOUT)
        return {
            "remaining_timers": len(scheduled_after),
            "expected_timer_time": timer2_time
        }

    handler = CallbackTimerHandler(unschedule_timer_callback)

    await asyncio.wait_for(client.subscribe(handler), timeout=DEFAULT_TIMEOUT)

    test_key = "unschedule-test-key"
    await asyncio.wait_for(client.send(topic, test_key, {"test": "data"}), timeout=DEFAULT_TIMEOUT)

    operation_result = await asyncio.wait_for(handler.results.receive(), timeout=DEFAULT_TIMEOUT)
    assert operation_result["success"]
    assert operation_result["remaining_timers"] == 1
    expected_timer_time = operation_result["expected_timer_time"]

    timer_event = await asyncio.wait_for(handler.timer_events.receive(), timeout=DEFAULT_TIMEOUT)

    time_diff = abs((timer_event["timer"].time - expected_timer_time).total_seconds())
    assert time_diff <= TIMER_TOLERANCE_SECONDS

async def test_timer_clear_and_schedule(client, random_topic_and_group):

    topic, _ = random_topic_and_group

    async def clear_and_schedule_callback(context: Context, message: Message):
        timer1_time = datetime.now(timezone.utc) + timedelta(seconds=5)
        timer2_time = datetime.now(timezone.utc) + timedelta(seconds=6)
        await asyncio.wait_for(context.schedule(timer1_time), timeout=DEFAULT_TIMEOUT)
        await asyncio.wait_for(context.schedule(timer2_time), timeout=DEFAULT_TIMEOUT)

        scheduled_before = await asyncio.wait_for(context.scheduled(), timeout=DEFAULT_TIMEOUT)
        assert len(scheduled_before) == 2

        new_timer_time = datetime.now(timezone.utc) + timedelta(seconds=2)
        await asyncio.wait_for(context.clear_and_schedule(new_timer_time), timeout=DEFAULT_TIMEOUT)

        scheduled_after = await asyncio.wait_for(context.scheduled(), timeout=DEFAULT_TIMEOUT)
        return {
            "remaining_timers": len(scheduled_after),
            "new_timer_time": new_timer_time
        }

    handler = CallbackTimerHandler(clear_and_schedule_callback)

    await asyncio.wait_for(client.subscribe(handler), timeout=DEFAULT_TIMEOUT)

    test_key = "clear-schedule-test-key"
    await asyncio.wait_for(client.send(topic, test_key, {"test": "data"}), timeout=DEFAULT_TIMEOUT)

    operation_result = await asyncio.wait_for(handler.results.receive(), timeout=DEFAULT_TIMEOUT)
    assert operation_result["success"]
    assert operation_result["remaining_timers"] == 1
    new_timer_time = operation_result["new_timer_time"]

    timer_event = await asyncio.wait_for(handler.timer_events.receive(), timeout=DEFAULT_TIMEOUT)

    time_diff = abs((timer_event["timer"].time - new_timer_time).total_seconds())
    assert time_diff <= TIMER_TOLERANCE_SECONDS

async def test_timer_clear_scheduled(client, random_topic_and_group):

    topic, _ = random_topic_and_group

    async def clear_scheduled_callback(context: Context, message: Message):
        timer1_time = datetime.now(timezone.utc) + timedelta(seconds=3)
        timer2_time = datetime.now(timezone.utc) + timedelta(seconds=4)
        timer3_time = datetime.now(timezone.utc) + timedelta(seconds=5)

        await asyncio.wait_for(context.schedule(timer1_time), timeout=DEFAULT_TIMEOUT)
        await asyncio.wait_for(context.schedule(timer2_time), timeout=DEFAULT_TIMEOUT)
        await asyncio.wait_for(context.schedule(timer3_time), timeout=DEFAULT_TIMEOUT)

        scheduled_before = await asyncio.wait_for(context.scheduled(), timeout=DEFAULT_TIMEOUT)
        assert len(scheduled_before) == 3

        await asyncio.wait_for(context.clear_scheduled(), timeout=DEFAULT_TIMEOUT)

        scheduled_after = await asyncio.wait_for(context.scheduled(), timeout=DEFAULT_TIMEOUT)
        return {"remaining_timers": len(scheduled_after)}

    handler = CallbackTimerHandler(clear_scheduled_callback)

    await asyncio.wait_for(client.subscribe(handler), timeout=DEFAULT_TIMEOUT)

    test_key = "clear-all-test-key"
    await asyncio.wait_for(client.send(topic, test_key, {"test": "data"}), timeout=DEFAULT_TIMEOUT)

    operation_result = await asyncio.wait_for(handler.results.receive(), timeout=DEFAULT_TIMEOUT)
    assert operation_result["success"]
    assert operation_result["remaining_timers"] == 0

    try:
        await asyncio.wait_for(handler.timer_events.receive(), timeout=6.0)
        assert False, "No timers should have fired after clearing"
    except asyncio.TimeoutError:
        pass


async def test_timer_scheduled_retrieval(client, random_topic_and_group):

    topic, _ = random_topic_and_group

    async def scheduled_retrieval_callback(context: Context, message: Message):
        scheduled_empty = await asyncio.wait_for(context.scheduled(), timeout=DEFAULT_TIMEOUT)
        assert len(scheduled_empty) == 0

        now = datetime.now(timezone.utc)
        timer_times = [
            now + timedelta(seconds=10),
            now + timedelta(seconds=20),
            now + timedelta(seconds=30)
        ]

        for timer_time in timer_times:
            await asyncio.wait_for(context.schedule(timer_time), timeout=DEFAULT_TIMEOUT)

        scheduled = await asyncio.wait_for(context.scheduled(), timeout=DEFAULT_TIMEOUT)
        return {
            "scheduled_count": len(scheduled),
            "expected_times": timer_times,
            "scheduled_times": scheduled
        }

    handler = CallbackTimerHandler(scheduled_retrieval_callback)

    await asyncio.wait_for(client.subscribe(handler), timeout=DEFAULT_TIMEOUT)

    test_key = "scheduled-retrieval-test"
    await asyncio.wait_for(client.send(topic, test_key, {"test": "data"}), timeout=DEFAULT_TIMEOUT)

    operation_result = await asyncio.wait_for(handler.results.receive(), timeout=DEFAULT_TIMEOUT)
    assert operation_result["success"]
    assert operation_result["scheduled_count"] == 3

    expected_times = operation_result["expected_times"]
    scheduled_times = operation_result["scheduled_times"]

    for expected_time in expected_times:
        found = False
        for scheduled_time in scheduled_times:
            time_diff = abs((scheduled_time - expected_time).total_seconds())
            if time_diff <= TIMER_TOLERANCE_SECONDS:
                found = True
                break
        assert found, f"Expected time {expected_time} not found in scheduled times"

