import test from "node:test";
import assert from "node:assert/strict";
import { setTimeout as sleep } from "node:timers/promises";
import { createMessageBroker } from "../async-message-broker.js";
import { createRetryQueue } from "../retry-queue.js";
import { requestWithRetry } from "../api-request-pipeline.js";
import { createAgentEngine } from "../agent-job-engine.js";
import { createMessageProcessor } from "../cancellable-worker.js";

test("broker isolates rejected subscribers and supports unsubscribe", async () => {
  const broker = createMessageBroker();
  const bad = () => {
    throw new Error("failed");
  };
  broker.subscribe("topic", bad);
  broker.subscribe("topic", async () => 42);
  const results = await broker.publish("topic", {});
  assert.deepEqual(
    results.map((r) => r.status),
    ["rejected", "fulfilled"],
  );
  broker.unsubscribe("topic", bad);
  assert.deepEqual(await broker.publish("topic", {}), [
    { status: "fulfilled", value: 42 },
  ]);
  assert.deepEqual(await broker.publish("missing", {}), []);
  assert.throws(() => broker.subscribe("topic", null), TypeError);
});

test("retry queue retries independently and records exhausted jobs", async () => {
  const attempts = new Map();
  const order = [];
  const queue = createRetryQueue(
    (job) => {
      const count = (attempts.get(job.id) ?? 0) + 1;
      attempts.set(job.id, count);
      order.push(job.id);
      if (job.id === "bad" || (job.id === "retry" && count < 2))
        throw "failure";
    },
    { concurrency: 1, maxRetries: 2, baseDelay: 5 },
  );
  queue.push({ id: "retry" });
  queue.push({ id: "good" });
  queue.push({ id: "bad" });
  await sleep(80);
  assert.equal(attempts.get("retry"), 2);
  assert.equal(attempts.get("bad"), 3);
  assert.equal(order[1], "good");
  const failed = queue.getDeadLetterQueue();
  assert.equal(failed[0].error, "failure");
  failed[0].error = "mutated";
  assert.equal(queue.getDeadLetterQueue()[0].error, "failure");
  assert.throws(
    () => createRetryQueue(() => {}, { concurrency: 0 }),
    RangeError,
  );
});

test("request pipeline retries synchronous errors and preserves successful values", async () => {
  let calls = 0;
  const result = await requestWithRetry(
    () => {
      if (++calls < 3) throw new Error("temporary");
      return { source: "timeout", value: 42 };
    },
    null,
    { maxRetries: 2, baseDelay: 1, timeout: 100 },
  );
  assert.equal(calls, 3);
  assert.deepEqual(result, { source: "timeout", value: 42 });
});

test("request timeout aborts actual attempt and cancellation stops retry waits", async () => {
  let attemptSignal;
  await assert.rejects(
    requestWithRetry(
      (input, { signal }) => {
        attemptSignal = signal;
        return new Promise(() => {});
      },
      null,
      { timeout: 5, maxRetries: 0 },
    ),
    { name: "TimeoutError" },
  );
  assert.equal(attemptSignal.aborted, true);
  const controller = new AbortController();
  let calls = 0;
  const pending = requestWithRetry(
    () => {
      calls++;
      throw new Error("temporary");
    },
    null,
    { signal: controller.signal, baseDelay: 10000 },
  );
  const rejected = assert.rejects(pending, { name: "AbortError" });
  await sleep(5);
  controller.abort();
  await rejected;
  assert.equal(calls, 1);
});

test("worker cancellation frees the slot and ignores a late result", async () => {
  let release;
  const processor = createMessageProcessor(1, (message) =>
    message === "A"
      ? new Promise((resolve) => {
          release = resolve;
        })
      : "B result",
  );
  const a = processor.push("A");
  const b = processor.push("B");
  await sleep(1);
  assert.equal(processor.cancel(a), true);
  await sleep(5);
  assert.equal(processor.getStatus(b).result, "B result");
  release("late");
  await sleep(1);
  assert.equal(processor.getStatus(a).status, "cancelled");
  assert.equal(processor.getStatus(a).result, null);
  assert.equal(processor.cancel(b), false);
});

test("agent engine handles queued cancellation and completes ordered steps", async () => {
  const engine = createAgentEngine({ concurrency: 1, baseDelay: 1 });
  const cancelled = engine.submit({ task: "cancel" });
  engine.cancel(cancelled);
  const id = engine.submit({ task: "JavaScript news" });
  const result = await engine.waitFor(id);
  assert.equal(result.status, "completed");
  assert.deepEqual(result.completedSteps, ["search", "summarize", "save"]);
  assert.equal((await engine.waitFor(cancelled)).status, "cancelled");
  result.result.topic = "mutated";
  assert.equal(engine.getStatus(id).result.topic, "JavaScript news");
  await assert.rejects(engine.waitFor("missing"), /Unknown/);
});
