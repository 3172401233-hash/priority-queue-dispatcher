const DEFAULT_POLICY = Object.freeze({
  concurrency: 4,
  weights: Object.freeze({ critical: 1000, high: 100, normal: 10, low: 1 }),
  aging: Object.freeze({ ratePerMs: 0.001, maxBoost: 500 }),
  deadline: Object.freeze({ urgencyWeight: 50, horizonMs: 60_000 }),
  tenant: Object.freeze({ deficitWeight: 20, shares: Object.freeze({}) }),
  minShare: Object.freeze({}),
  fairnessWindow: 50,
  runningTimeoutFactor: 4,
});

function clamp(value, lo, hi) {
  return Math.min(hi, Math.max(lo, value));
}

function mergePolicy(base, patch) {
  const merged = { ...base, ...patch };
  for (const key of ['weights', 'aging', 'deadline', 'tenant', 'minShare']) {
    if (patch[key] !== undefined || base[key] !== undefined) {
      merged[key] = { ...(base[key] ?? {}), ...(patch[key] ?? {}) };
    }
  }
  if (patch.tenant?.shares !== undefined || base.tenant?.shares !== undefined) {
    merged.tenant.shares = { ...(base.tenant?.shares ?? {}), ...(patch.tenant?.shares ?? {}) };
  }
  return merged;
}

/**
 * Priority dispatcher.
 *
 * Scheduling score = priority weight + aging boost + deadline urgency + tenant deficit boost.
 * A per-priority-class minimum share floor (minShare) is enforced over a sliding
 * window of recent dispatches, so high-priority floods can preempt waiting quota
 * but can never fully starve a backlogged low-priority class.
 */
export class PriorityDispatcher {
  constructor(options = {}) {
    const { policy: policyPatch, ...rest } = options;
    this.policy = mergePolicy(mergePolicy(DEFAULT_POLICY, rest), policyPatch ?? {});
    this.queue = new Map();
    this.running = new Map();
    this.history = [];
    this.dispatchedLog = [];
    this._seq = 0;
  }

  updatePolicy(patch = {}) {
    this.policy = mergePolicy(this.policy, patch);
    return this.policy;
  }

  enqueue(task, now = Date.now()) {
    const { id, priority, deadline = null, estimatedDuration, tenant = 'default' } = task;
    if (id == null) throw new Error('task.id is required');
    if (this.queue.has(id) || this.running.has(id)) throw new Error(`duplicate task id: ${id}`);
    if (!(priority in this.policy.weights)) {
      throw new Error(`unknown priority "${priority}"; known: ${Object.keys(this.policy.weights).join(', ')}`);
    }
    if (!(estimatedDuration > 0)) throw new Error('task.estimatedDuration must be > 0');
    const record = { id, priority, deadline, estimatedDuration, tenant, enqueuedAt: now, seq: this._seq++ };
    this.queue.set(id, record);
    return record;
  }

  cancel(id) {
    return this.queue.delete(id);
  }

  complete(id, now = Date.now()) {
    const task = this.running.get(id);
    if (!task) return false;
    this.running.delete(id);
    this._log(task, now, 'completed', `finished after ${now - task.startedAt}ms`);
    return true;
  }

  /** Fail running tasks that exceeded estimatedDuration * runningTimeoutFactor. */
  reapTimeouts(now = Date.now()) {
    const timedOut = [];
    for (const task of this.running.values()) {
      const budget = task.estimatedDuration * this.policy.runningTimeoutFactor;
      if (now - task.startedAt > budget) {
        this.running.delete(task.id);
        const reason = `running timeout: elapsed ${now - task.startedAt}ms > budget ${budget}ms` +
          ` (${this.policy.runningTimeoutFactor}x estimate ${task.estimatedDuration}ms)`;
        this._log(task, now, 'running-timeout', reason);
        timedOut.push({ task, reason });
      }
    }
    return timedOut;
  }

  _log(task, at, action, reason) {
    const entry = { id: task.id, priority: task.priority, tenant: task.tenant, at, action, reason };
    this.history.push(entry);
    if (action === 'dispatched') this.dispatchedLog.push(entry);
    return entry;
  }

  _windowShares(keyFn) {
    const window = this.dispatchedLog.slice(-this.policy.fairnessWindow);
    const counts = new Map();
    for (const entry of window) {
      const key = keyFn(entry);
      counts.set(key, (counts.get(key) ?? 0) + 1);
    }
    const total = window.length;
    return { shares: (key) => (total === 0 ? 0 : (counts.get(key) ?? 0) / total), total };
  }

  _score(task, now, tenantShareOf) {
    const p = this.policy;
    const base = p.weights[task.priority];
    const waitedMs = Math.max(0, now - task.enqueuedAt);
    const aging = Math.min(p.aging.maxBoost, waitedMs * p.aging.ratePerMs);
    let urgency = 0;
    if (task.deadline != null) {
      const remaining = task.deadline - now;
      urgency = p.deadline.urgencyWeight * (1 - clamp(remaining / p.deadline.horizonMs, 0, 1));
    }
    let tenantBoost = 0;
    const configured = p.tenant.shares[task.tenant];
    if (configured !== undefined) {
      tenantBoost = p.tenant.deficitWeight * (configured - tenantShareOf(task.tenant));
    }
    const total = base + aging + urgency + tenantBoost;
    return { base, aging, urgency, tenantBoost, total };
  }

  _formatScore(s) {
    const parts = [`base ${s.base}`];
    if (s.aging) parts.push(`aging +${s.aging.toFixed(2)}`);
    if (s.urgency) parts.push(`urgency +${s.urgency.toFixed(2)}`);
    if (s.tenantBoost) parts.push(`tenant ${s.tenantBoost > 0 ? '+' : ''}${s.tenantBoost.toFixed(2)}`);
    return `score ${s.total.toFixed(2)} (${parts.join(', ')})`;
  }

  _deficientClasses(classShareOf) {
    const pendingByClass = new Map();
    for (const task of this.queue.values()) {
      if (!pendingByClass.has(task.priority)) pendingByClass.set(task.priority, []);
      pendingByClass.get(task.priority).push(task);
    }
    const deficient = [];
    for (const [priority, floor] of Object.entries(this.policy.minShare)) {
      const pending = pendingByClass.get(priority);
      if (!pending || pending.length === 0) continue;
      const share = classShareOf(priority);
      if (share < floor) deficient.push({ priority, floor, share, deficit: floor - share, pending });
    }
    deficient.sort((a, b) => b.deficit - a.deficit);
    return deficient;
  }

  /**
   * Pick the next batch. Returns { batch, expired }.
   * batch: [{ task, score, reason }] — one entry per newly started task.
   * expired: [{ task, reason }] — queued tasks dropped because their deadline passed.
   */
  dispatch(now = Date.now()) {
    const expired = [];
    for (const task of [...this.queue.values()]) {
      if (task.deadline != null && task.deadline <= now) {
        this.queue.delete(task.id);
        const reason = `deadline expired while queued: deadline ${task.deadline} <= now ${now}` +
          ` (waited ${now - task.enqueuedAt}ms)`;
        this._log(task, now, 'expired', reason);
        expired.push({ task, reason });
      }
    }

    const batch = [];
    let freeSlots = this.policy.concurrency - this.running.size;
    if (freeSlots <= 0 || this.queue.size === 0) return { batch, expired };

    while (freeSlots > 0 && this.queue.size > 0) {
      const { shares: classShareOf } = this._windowShares((e) => e.priority);
      const { shares: tenantShareOf } = this._windowShares((e) => e.tenant);

      const deficient = this._deficientClasses(classShareOf);
      let chosen;
      let reason;

      if (deficient.length > 0) {
        const target = deficient[0];
        let best = null;
        let bestScore = null;
        for (const task of target.pending) {
          const s = this._score(task, now, tenantShareOf);
          if (!best || s.total > bestScore.total || (s.total === bestScore.total && task.seq < best.seq)) {
            best = task;
            bestScore = s;
          }
        }
        chosen = best;
        reason = `min-share floor: class "${target.priority}" window share ${target.share.toFixed(3)}` +
          ` < floor ${target.floor}; guaranteed slot. ${this._formatScore(bestScore)}`;
      } else {
        let best = null;
        let bestScore = null;
        for (const task of this.queue.values()) {
          const s = this._score(task, now, tenantShareOf);
          if (!best || s.total > bestScore.total || (s.total === bestScore.total && task.seq < best.seq)) {
            best = task;
            bestScore = s;
          }
        }
        chosen = best;
        const jumped = [...this.queue.values()].filter(
          (t) => t.seq < chosen.seq && this.policy.weights[t.priority] < this.policy.weights[chosen.priority],
        ).length;
        reason = `highest ${this._formatScore(bestScore)}`;
        if (jumped > 0) {
          reason += `; preempted waiting quota of ${jumped} older lower-priority task(s)`;
        }
      }

      this.queue.delete(chosen.id);
      chosen.startedAt = now;
      this.running.set(chosen.id, chosen);
      const score = this._score(chosen, now, tenantShareOf);
      this._log(chosen, now, 'dispatched', reason);
      batch.push({ task: chosen, score: score.total, reason });
      freeSlots -= 1;
    }

    return { batch, expired };
  }

  stats() {
    return {
      queued: this.queue.size,
      running: this.running.size,
      concurrency: this.policy.concurrency,
      dispatched: this.dispatchedLog.length,
      history: this.history.length,
    };
  }
}

export { DEFAULT_POLICY };
