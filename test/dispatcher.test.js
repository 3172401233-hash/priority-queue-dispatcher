import { test } from 'node:test';
import assert from 'node:assert/strict';
import { PriorityDispatcher } from '../src/dispatcher.js';

function makeDispatcher(overrides = {}) {
  return new PriorityDispatcher({
    concurrency: 2,
    weights: { critical: 1000, high: 100, normal: 10, low: 1 },
    aging: { ratePerMs: 0.01, maxBoost: 500 },
    deadline: { urgencyWeight: 50, horizonMs: 10_000 },
    tenant: { deficitWeight: 20, shares: {} },
    minShare: {},
    fairnessWindow: 10,
    runningTimeoutFactor: 4,
    ...overrides,
  });
}

test('empty queue: dispatch returns empty batch and no expirations', () => {
  const d = makeDispatcher();
  const { batch, expired } = d.dispatch(1000);
  assert.deepEqual(batch, []);
  assert.deepEqual(expired, []);
  assert.equal(d.stats().queued, 0);
});

test('concurrency budget caps the batch size', () => {
  const d = makeDispatcher({ concurrency: 3 });
  for (let i = 0; i < 10; i++) {
    d.enqueue({ id: `t${i}`, priority: 'normal', estimatedDuration: 100 }, 0);
  }
  const { batch } = d.dispatch(0);
  assert.equal(batch.length, 3);
  assert.equal(d.stats().running, 3);
  const again = d.dispatch(0);
  assert.equal(again.batch.length, 0, 'no free slots while running');
});

test('priority order with per-dispatch reasons and preemption note', () => {
  const d = makeDispatcher({ concurrency: 2 });
  d.enqueue({ id: 'low-1', priority: 'low', estimatedDuration: 100 }, 0);
  d.enqueue({ id: 'normal-1', priority: 'normal', estimatedDuration: 100 }, 10);
  d.enqueue({ id: 'high-1', priority: 'high', estimatedDuration: 100 }, 20);
  const { batch } = d.dispatch(30);
  assert.deepEqual(batch.map((b) => b.task.id), ['high-1', 'normal-1']);
  for (const entry of batch) {
    assert.match(entry.reason, /score/);
  }
  assert.match(batch[0].reason, /preempted waiting quota of 2 older lower-priority/);
});

test('aging lets an old low-priority task overtake a fresh normal task', () => {
  const d = makeDispatcher({ concurrency: 1 });
  d.enqueue({ id: 'old-low', priority: 'low', estimatedDuration: 100 }, 0);
  const now = 10_000; // aging boost = 10000 * 0.01 = 100 > normal base 10
  d.enqueue({ id: 'fresh-normal', priority: 'normal', estimatedDuration: 100 }, now);
  const { batch } = d.dispatch(now);
  assert.equal(batch[0].task.id, 'old-low');
  assert.match(batch[0].reason, /aging \+100/);
});

test('sustained high-priority flood cannot starve low class below its floor', () => {
  const d = makeDispatcher({ concurrency: 1, minShare: { low: 0.2 }, fairnessWindow: 10 });
  let now = 0;
  let lowCount = 0;
  let highCount = 0;
  let maxGap = 0;
  let gap = 0;
  let lowSeq = 0;
  d.enqueue({ id: 'low-0', priority: 'low', estimatedDuration: 10 }, now);
  for (let round = 0; round < 100; round++) {
    now += 10;
    d.enqueue({ id: `high-${round}`, priority: 'high', estimatedDuration: 10 }, now);
    const { batch } = d.dispatch(now);
    assert.ok(batch.length <= 1);
    if (batch.length === 0) continue;
    const task = batch[0].task;
    d.complete(task.id, now + 10);
    if (task.priority === 'low') {
      lowCount += 1;
      maxGap = Math.max(maxGap, gap);
      gap = 0;
      assert.match(batch[0].reason, /min-share floor/);
      lowSeq += 1;
      d.enqueue({ id: `low-${lowSeq}`, priority: 'low', estimatedDuration: 10 }, now);
    } else {
      highCount += 1;
      gap += 1;
    }
  }
  assert.ok(lowCount >= 15, `expected low to run >= 15/100 times, got ${lowCount}`);
  assert.ok(highCount > lowCount, 'high priority still dominates throughput');
  assert.ok(maxGap <= 5, `low never waits more than 5 dispatches, max gap was ${maxGap}`);
});

test('queued task past its deadline expires instead of dispatching', () => {
  const d = makeDispatcher({ concurrency: 1 });
  d.enqueue({ id: 'stuck', priority: 'critical', estimatedDuration: 500 }, 0);
  d.enqueue({ id: 'impatient', priority: 'high', estimatedDuration: 10, deadline: 100 }, 0);
  d.dispatch(0); // starts "stuck", occupying the only slot
  const { batch, expired } = d.dispatch(200);
  assert.equal(batch.length, 0);
  assert.equal(expired.length, 1);
  assert.equal(expired[0].task.id, 'impatient');
  assert.match(expired[0].reason, /deadline expired while queued/);
  assert.equal(d.stats().queued, 0);
});

test('running task exceeding timeout budget is reaped and frees its slot', () => {
  const d = makeDispatcher({ concurrency: 1, runningTimeoutFactor: 2 });
  d.enqueue({ id: 'slow', priority: 'high', estimatedDuration: 100 }, 0);
  d.enqueue({ id: 'next', priority: 'normal', estimatedDuration: 50 }, 0);
  d.dispatch(0);
  assert.equal(d.stats().running, 1);
  const timedOut = d.reapTimeouts(201); // budget = 100 * 2 = 200
  assert.equal(timedOut.length, 1);
  assert.equal(timedOut[0].task.id, 'slow');
  assert.match(timedOut[0].reason, /running timeout/);
  const { batch } = d.dispatch(201);
  assert.equal(batch[0].task.id, 'next');
});

test('tenant fair-share steers dispatch ratio toward configured shares', () => {
  const d = makeDispatcher({
    concurrency: 1,
    fairnessWindow: 20,
    tenant: { deficitWeight: 200, shares: { big: 0.8, small: 0.2 } },
  });
  let now = 0;
  const counts = { big: 0, small: 0 };
  for (let round = 0; round < 100; round++) {
    now += 10;
    d.enqueue({ id: `big-${round}`, priority: 'normal', estimatedDuration: 10, tenant: 'big' }, now);
    d.enqueue({ id: `small-${round}`, priority: 'normal', estimatedDuration: 10, tenant: 'small' }, now);
    const { batch } = d.dispatch(now);
    if (batch.length === 0) continue;
    counts[batch[0].task.tenant] += 1;
    d.complete(batch[0].task.id, now + 5);
  }
  const total = counts.big + counts.small;
  assert.ok(counts.big / total > 0.65, `big tenant share ${counts.big}/${total}`);
  assert.ok(counts.small / total > 0.1, `small tenant share ${counts.small}/${total}`);
});

test('policy change takes effect on subsequent dispatches', () => {
  const d = makeDispatcher({ concurrency: 1 });
  d.enqueue({ id: 'a-low', priority: 'low', estimatedDuration: 10 }, 0);
  d.enqueue({ id: 'a-high', priority: 'high', estimatedDuration: 10 }, 0);
  let { batch } = d.dispatch(0);
  assert.equal(batch[0].task.id, 'a-high');
  d.complete('a-high', 10);

  d.updatePolicy({ weights: { low: 5000 } });
  ({ batch } = d.dispatch(10));
  assert.equal(batch[0].task.id, 'a-low', 'reweighted low now wins');
  d.complete('a-low', 20);

  d.enqueue({ id: 'b-low', priority: 'low', estimatedDuration: 10 }, 20);
  d.enqueue({ id: 'b-high', priority: 'high', estimatedDuration: 10 }, 20);
  d.updatePolicy({ minShare: { low: 0.5 } });
  d.dispatch(20);
  d.complete('b-low', 30);
  d.updatePolicy({ minShare: { low: 0 } });
  ({ batch } = d.dispatch(30));
  assert.equal(batch[0].task.id, 'b-high', 'floor removed, weight order restored');
});

test('fairness window: floor stops firing once the class share recovers', () => {
  const d = makeDispatcher({ concurrency: 1, minShare: { low: 0.3 }, fairnessWindow: 4 });
  let now = 0;
  const reasons = [];
  d.enqueue({ id: 'low-0', priority: 'low', estimatedDuration: 10 }, 0);
  let lowSeq = 0;
  for (let round = 0; round < 12; round++) {
    now += 10;
    d.enqueue({ id: `high-${round}`, priority: 'high', estimatedDuration: 10 }, now);
    const { batch } = d.dispatch(now);
    if (batch.length === 0) continue;
    reasons.push(batch[0].reason);
    d.complete(batch[0].task.id, now + 5);
    if (batch[0].task.priority === 'low') {
      lowSeq += 1;
      d.enqueue({ id: `low-${lowSeq}`, priority: 'low', estimatedDuration: 10 }, now);
    }
  }
  const floorRuns = reasons.filter((r) => r.includes('min-share floor')).length;
  assert.ok(floorRuns >= 2 && floorRuns <= 6, `floor fired ${floorRuns} times in 12 rounds`);
});

test('every dispatch and expiration is recorded with a reason in history', () => {
  const d = makeDispatcher({ concurrency: 1 });
  d.enqueue({ id: 'x', priority: 'high', estimatedDuration: 10 }, 0);
  d.enqueue({ id: 'y', priority: 'low', estimatedDuration: 10, deadline: 5 }, 0);
  d.dispatch(0);
  d.complete('x', 10);
  d.dispatch(10);
  const actions = d.history.map((h) => h.action);
  assert.deepEqual(actions, ['dispatched', 'completed', 'expired']);
  for (const entry of d.history) {
    assert.ok(entry.reason.length > 0, `entry ${entry.action} has a reason`);
  }
});
