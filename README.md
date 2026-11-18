# priority-queue-dispatcher
JavaScript 优先级任务分发库，处理老化、公平份额、截止时间和队列背压

## 快速开始

```js
import { PriorityDispatcher } from './src/dispatcher.js';

const dispatcher = new PriorityDispatcher({
  concurrency: 4,                                  // 并发预算
  weights: { critical: 1000, high: 100, normal: 10, low: 1 },
  aging: { ratePerMs: 0.001, maxBoost: 500 },      // 等待越久加分越多，封顶 500
  deadline: { urgencyWeight: 50, horizonMs: 60_000 },
  tenant: { deficitWeight: 20, shares: { teamA: 0.7, teamB: 0.3 } },
  minShare: { low: 0.1 },                          // low 类的最低份额地板
  fairnessWindow: 50,                              // 在最近 50 次派发内核算份额
  runningTimeoutFactor: 4,                         // 运行超过 4x 预计耗时视为超时
});

dispatcher.enqueue({ id: 't1', priority: 'high', deadline: Date.now() + 5000, estimatedDuration: 200, tenant: 'teamA' });

const { batch, expired } = dispatcher.dispatch();
for (const { task, score, reason } of batch) {
  console.log(task.id, score, reason); // 每次派发都带可审计的原因
}
```

## 调度模型

每个等待任务的得分：

```
score = 优先级权重 (weights[priority])
      + aging 加分  (min(等待时长 × ratePerMs, maxBoost))
      + 截止时间紧迫度 (deadline 越近越高，horizonMs 内线性增长)
      + 租户赤字补偿 (deficitWeight × (配置份额 − 窗口内实际份额))
```

派发顺序：

1. **过期清理**：队列中 `deadline <= now` 的任务直接丢弃，记入 `expired` 并附原因。
2. **最低份额地板**：若某优先级类在公平窗内的实际份额低于 `minShare` 且有积压，
   该类以最高赤字优先获得槽位（原因标记 `min-share floor`）。
3. **得分排序**：其余槽位按 score 降序；高优先级任务插队时，
   原因会注明 `preempted waiting quota of N older lower-priority task(s)`。

运行中的任务通过 `complete(id)` 释放槽位；`reapTimeouts(now)` 回收运行时间超过
`estimatedDuration × runningTimeoutFactor` 的任务并释放并发预算。

政策可热更新：`updatePolicy({ weights, minShare, ... })` 之后的派发立即生效
（见测试 "policy change takes effect"）。

## 公平性与饥饿边界

- **抢占边界**：高优先级任务凭权重分抢占等待配额，但 `minShare` 地板优先于得分排序，
  因此低优先级类不会被完全耗尽。
- **饥饿上界**：设某类最低份额为 `m`、公平窗为 `W`。一旦该类在窗口内份额跌破 `m`
  且有积压，它在下一次派发中必然获得槽位（除非有赤字更大的类）。因此持续积压的类
  两次被派发之间的最大间隔不超过 `K` 次派发（`K` = 同时处于赤字状态的类别数，
  上界为优先级类别总数），且在任意连续 `W` 次派发中至少获得约 `⌊m × W⌋` 个槽位。
  测试 "sustained high-priority flood" 验证：100 轮持续高优输入下，
  `minShare: 0.2` 的 low 类实际运行 ≥ 15 次且最大间隔 ≤ 5。
- **aging 边界**：低优先级任务的 aging 加分封顶 `maxBoost`，因此它最终能超越
  权重差小于 `maxBoost` 的新到任务，但永远无法超越权重差更大的紧急类
  （如 `critical` 与 `low` 差 999 > 500），保证紧急故障恢复任务的 SLA。
- **租户公平**：配置 `tenant.shares` 后，窗口内低于份额的租户获得正向补偿分，
  超出份额的被惩罚，长期派发比例收敛到配置份额。
- **超时语义**：等待超期（deadline 已过）的任务被丢弃而非派发；
  运行超时（超过预计耗时 × 系数）由 `reapTimeouts` 强制回收，避免泄漏并发预算。

## 测试

```sh
node --test
```

覆盖：持续高优输入下的饥饿边界、等待/运行两种超时、空队列、公平窗份额恢复、
政策热更新、并发预算上限、aging 反超、租户份额、以及每次派发的可审计原因。
