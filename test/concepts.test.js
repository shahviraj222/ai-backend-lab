import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import test from "node:test";
import { runTasks } from "../task-scheduler.js";
import { createRequestCache } from "../request-deduplication.js";
import { createEventEmitter } from "../event-emitter.js";
import { debounce } from "../debounce.js";
import { retry } from "../retry.js";
import { createRateLimiter } from "../rate-limiter.js";
import { createLRUCache } from "../lru-cache.js";
import { withTimeout } from "../timeout.js";
import { createBatcher } from "../batch-processor.js";
import { createCircuitBreaker } from "../circuit-breaker.js";
import { createAsyncQueue } from "../async-queue.js";
import { createMessageProcessor } from "../concurrent-message-processor.js";
import {
  createAsyncQueue as createClosableQueue,
  createMessageProcessor as createGracefulProcessor,
  QueueClosedError,
} from "../graceful-shutdown.js";
import {
  createAsyncQueue as createBoundedQueue,
  createMessageProcessor as createBoundedProcessor,
  QueueClosedError as BoundedQueueClosedError,
} from "../backpressure.js";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((onResolve, onReject) => {
    resolve = onResolve;
    reject = onReject;
  });
  return { promise, resolve, reject };
}

test("scheduler limits concurrency and preserves input order", async () => {
  let active = 0;
  let peak = 0;
  const gate = deferred();
  const tasks = [0, 1, 2, 3].map((value) => async () => {
    active++;
    peak = Math.max(peak, active);
    await gate.promise;
    active--;
    return value;
  });

  const result = runTasks(tasks, 2);
  assert.equal(active, 2);
  gate.resolve();
  assert.deepEqual(await result, [0, 1, 2, 3]);
  assert.equal(peak, 2);
});

test("scheduler continues after failures and handles empty input", async () => {
  const result = await runTasks(
    [
      () => {
        throw new Error("failed");
      },
      () => "done",
    ],
    5,
  );
  assert.deepEqual(result, [{ success: false, message: "failed" }, "done"]);
  assert.deepEqual(await runTasks([], 2), []);
  await assert.rejects(runTasks([], 0), RangeError);
  await assert.rejects(runTasks([42], 1), TypeError);
});

test("request cache shares in-flight and successful requests", async () => {
  let calls = 0;
  const get = createRequestCache(async (key) => {
    calls++;
    return { key };
  });
  const first = get("__proto__");
  assert.equal(get("__proto__"), first);
  assert.deepEqual(await first, { key: "__proto__" });
  assert.equal(get("__proto__"), first);
  await get("another");
  assert.equal(calls, 2);
});

test("failed cached requests can be retried, including synchronous throws", async () => {
  for (const asynchronous of [false, true]) {
    let calls = 0;
    const failure = new Error("offline");
    const get = createRequestCache(() => {
      if (++calls === 1) {
        if (asynchronous) return Promise.reject(failure);
        throw failure;
      }
      return "recovered";
    });
    const first = get(1);
    assert.equal(get(1), first);
    await assert.rejects(first, (error) => error === failure);
    assert.equal(await get(1), "recovered");
    assert.equal(calls, 2);
  }
});

test("emitter removes listeners and isolates instances", () => {
  const emitter = createEventEmitter();
  let calls = 0;
  const listener = () => calls++;
  emitter.off("missing", listener);
  emitter.on("__proto__", listener);
  createEventEmitter().emit("__proto__");
  emitter.emit("__proto__");
  emitter.off("__proto__", listener);
  emitter.emit("__proto__");
  assert.equal(calls, 1);
});

test("once does not skip listeners or repeat during nested emissions", () => {
  const emitter = createEventEmitter();
  const calls = [];
  emitter.once("message", () => calls.push("once"));
  emitter.on("message", () => calls.push("regular"));
  emitter.emit("message");
  emitter.emit("message");
  assert.deepEqual(calls, ["once", "regular", "regular"]);

  const nested = createEventEmitter();
  let onceCalls = 0;
  let recurse = true;
  nested.on("event", () => {
    if (recurse) {
      recurse = false;
      nested.emit("event");
    }
  });
  nested.once("event", () => onceCalls++);
  nested.emit("event");
  assert.equal(onceCalls, 1);
});

test("debounce uses the latest arguments and preserves this", async () => {
  const completed = deferred();
  const receiver = {
    prefix: "Result",
    run: debounce(function (...args) {
      completed.resolve([this.prefix, ...args]);
    }, 10),
  };
  receiver.run("old", 1);
  receiver.run("new", 2);
  assert.deepEqual(await completed.promise, ["Result", "new", 2]);
});

test("retry stops on success and preserves the final error", async () => {
  let calls = 0;
  assert.equal(
    await retry(
      () => {
        if (++calls < 3) throw new Error("temporary");
        return "ok";
      },
      3,
      0,
    ),
    "ok",
  );
  assert.equal(calls, 3);

  const failure = new Error("offline");
  calls = 0;
  await assert.rejects(
    retry(
      () => {
        calls++;
        throw failure;
      },
      2,
      0,
    ),
    (error) => error === failure,
  );
  assert.equal(calls, 2);
  await assert.rejects(
    retry(() => {}, 0, 0),
    RangeError,
  );
});

test("rate limiter isolates users and uses a rolling window", (t) => {
  let now = 0;
  t.mock.method(performance, "now", () => now);
  const allow = createRateLimiter(2, 20);
  assert.equal(allow("__proto__"), true);
  now = 5;
  assert.equal(allow("__proto__"), true);
  assert.equal(allow("__proto__"), false);
  assert.equal(allow("another"), true);
  now = 20;
  assert.equal(allow("__proto__"), true);
  assert.equal(allow("__proto__"), false);
  now = 25;
  assert.equal(allow("__proto__"), true);
});

test("LRU starts empty and evicts the least recently used entry", () => {
  const cache = createLRUCache(3);
  assert.equal(cache.get("A"), -1);
  cache.put("A", 100);
  cache.put("B", 200);
  cache.put("C", 300);
  assert.equal(cache.get("A"), 100);
  cache.put("D", 400);
  assert.equal(cache.get("B"), -1);
  assert.equal(cache.get("C"), 300);
  assert.equal(cache.get("D"), 400);
});

test("LRU updates refresh recency without consuming another slot", () => {
  const cache = createLRUCache(2);
  cache.put("__proto__", 1);
  cache.put("B", 2);
  cache.put("__proto__", 3);
  cache.put("C", 4);
  assert.equal(cache.get("B"), -1);
  assert.equal(cache.get("__proto__"), 3);
});

test("timeout returns values, propagates failures, and expires slow work", async () => {
  assert.equal(await withTimeout(() => 42, 100), 42);
  const failure = new Error("offline");
  await assert.rejects(
    withTimeout(() => {
      throw failure;
    }, 100),
    (e) => e === failure,
  );
  await assert.rejects(
    withTimeout(() => new Promise(() => {}), 5),
    /API timeout/,
  );
});

test("settled timeout operations do not keep Node alive", () => {
  const url = new URL("../timeout.js", import.meta.url).href;
  const script = `import { withTimeout } from ${JSON.stringify(url)};
    await withTimeout(() => "done", 60000);
    await withTimeout(() => { throw new Error("failed"); }, 60000).catch(() => {});`;
  execFileSync(process.execPath, ["--input-type=module", "-e", script], {
    timeout: 3000,
  });
});

test("batcher sends full batches immediately and partial batches after waiting", async () => {
  const batches = [];
  const partialSent = deferred();
  const batcher = createBatcher(3, 20, (batch) => {
    batches.push(batch);
    if (batches.length === 2) partialSent.resolve();
  });
  batcher.add("A");
  batcher.add("B");
  assert.equal(batches.length, 0);
  batcher.add("C");
  assert.deepEqual(batches, [["A", "B", "C"]]);
  const started = performance.now();
  batcher.add("D");
  batcher.add("E");
  await partialSent.promise;
  assert.ok(performance.now() - started >= 15);
  assert.deepEqual(batches, [
    ["A", "B", "C"],
    ["D", "E"],
  ]);
});

test("batcher flush clears its timer and reports sender errors", async () => {
  const batches = [];
  const batcher = createBatcher(3, 10, (batch) => batches.push(batch));
  batcher.add("A");
  await batcher.flush();
  await sleep(20);
  assert.deepEqual(batches, [["A"]]);

  const reported = deferred();
  const failure = new Error("send failed");
  const failing = createBatcher(
    1,
    10,
    async () => {
      throw failure;
    },
    reported.resolve,
  );
  failing.add("B");
  assert.equal(await reported.promise, failure);

  const manual = createBatcher(3, 100, () => {
    throw failure;
  });
  manual.add("C");
  await assert.rejects(manual.flush(), (error) => error === failure);
});

test("circuit breaker blocks during cooldown and recovers", async (t) => {
  let now = 0;
  t.mock.method(performance, "now", () => now);
  let calls = 0;
  let available = false;
  const call = createCircuitBreaker(
    () => {
      calls++;
      if (!available) throw new Error("offline");
      return "ok";
    },
    2,
    20,
  );
  await assert.rejects(call(), /offline/);
  await assert.rejects(call(), /offline/);
  await assert.rejects(call(), /OPEN/);
  assert.equal(calls, 2);
  now = 20;
  available = true;
  assert.equal(await call(), "ok");
  assert.equal(await call(), "ok");
});

test("circuit breaker allows one recovery probe and reopens if it fails", async () => {
  let response = Promise.reject(new Error("offline"));
  const call = createCircuitBreaker(() => response, 1, 0);
  await assert.rejects(call(), /offline/);
  const probe = deferred();
  response = probe.promise;
  const first = call();
  await assert.rejects(call(), /already running/);
  probe.reject(new Error("still offline"));
  await assert.rejects(first, /still offline/);
  response = Promise.resolve("recovered");
  assert.equal(await call(), "recovered");
});

test("circuit breaker ignores late failures from before recovery", async (t) => {
  let now = 0;
  t.mock.method(performance, "now", () => now);
  const old = deferred();
  const call = createCircuitBreaker(
    (request) => {
      if (request === "old") return old.promise;
      if (request === "fail") throw new Error("offline");
      return "ok";
    },
    1,
    20,
  );
  const oldCall = call("old");
  const oldRejection = assert.rejects(oldCall, /late/);
  await assert.rejects(call("fail"), /offline/);
  now = 20;
  assert.equal(await call("probe"), "ok");
  old.reject(new Error("late"));
  await oldRejection;
  assert.equal(await call("normal"), "ok");
});

test("async queue preserves FIFO for buffered items and waiting consumers", async () => {
  const queue = createAsyncQueue();
  queue.push("A");
  queue.push("B");
  assert.equal(await queue.next(), "A");
  assert.equal(await queue.next(), "B");
  const first = queue.next();
  const second = queue.next();
  queue.push(undefined);
  queue.push("D");
  assert.deepEqual(await Promise.all([first, second]), [undefined, "D"]);
});

test("message workers enforce concurrency and accept work after becoming idle", async () => {
  let active = 0;
  let peak = 0;
  const started = [];
  const gates = [deferred(), deferred(), deferred()];
  const initialDone = deferred();
  const laterDone = deferred();
  let finished = 0;
  const processor = createMessageProcessor(2, async (index) => {
    started.push(index);
    active++;
    peak = Math.max(peak, active);
    if (index < 3) await gates[index].promise;
    active--;
    if (++finished === 3) initialDone.resolve();
    if (index === 3) laterDone.resolve();
  });
  processor.push(0);
  processor.push(1);
  processor.push(2);
  await Promise.resolve();
  assert.deepEqual(started, [0, 1]);
  gates.forEach((gate) => gate.resolve());
  await initialDone.promise;
  await Promise.resolve();
  processor.push(3);
  await laterDone.promise;
  assert.deepEqual(started, [0, 1, 2, 3]);
  assert.equal(peak, 2);
});

test("a failed message does not stop its worker", async (t) => {
  t.mock.method(console, "error", () => {});
  const done = deferred();
  const processor = createMessageProcessor(1, (message) => {
    if (message === "bad") throw new Error("invalid message");
    done.resolve(message);
  });
  processor.push("bad");
  processor.push("good");
  assert.equal(await done.promise, "good");
});

test("closing a queue drains accepted work and rejects idle consumers", async () => {
  const queue = createClosableQueue();
  queue.push("accepted");
  queue.close();
  queue.close();
  assert.equal(await queue.next(), "accepted");
  await assert.rejects(queue.next(), QueueClosedError);
  assert.throws(() => queue.push("late"), QueueClosedError);

  const idle = createClosableQueue();
  const waiting = assert.rejects(idle.next(), QueueClosedError);
  idle.close();
  await waiting;
});

test("graceful shutdown waits for running and queued messages, including failures", async (t) => {
  t.mock.method(console, "error", () => {});
  const gate = deferred();
  const processed = [];
  const processor = createGracefulProcessor(2, async (message) => {
    await gate.promise;
    processed.push(message);
    if (message === "B") throw new Error("failed");
  });
  for (const message of ["A", "B", "C", "D"]) processor.push(message);
  let stopped = false;
  const shutdown = processor.shutdown().then(() => {
    stopped = true;
  });
  await Promise.resolve();
  assert.equal(stopped, false);
  assert.throws(() => processor.push("E"), QueueClosedError);
  gate.resolve();
  await shutdown;
  await processor.shutdown();
  assert.deepEqual(processed, ["A", "B", "C", "D"]);
});

test("backpressure waits for admission and bounds pending producers", async () => {
  const queue = createBoundedQueue(1, 1);
  await queue.push("A");
  let admitted = false;
  const pending = queue.push("B").then(() => {
    admitted = true;
  });
  await Promise.resolve();
  assert.equal(admitted, false);
  await assert.rejects(queue.push("overflow"), /too many waiting producers/);
  assert.equal(await queue.next(), "A");
  await pending;
  assert.equal(admitted, true);
  assert.equal(await queue.next(), "B");
  queue.close();
});

test("backpressure preserves producer FIFO", async () => {
  const queue = createBoundedQueue(1, 2);
  await queue.push("A");
  const second = queue.push("B");
  const third = queue.push("C");
  assert.equal(await queue.next(), "A");
  await second;
  assert.equal(await queue.next(), "B");
  await third;
  assert.equal(await queue.next(), "C");
});

test("bounded queue shutdown rejects unadmitted work and drains accepted work", async () => {
  const queue = createBoundedQueue(1);
  await queue.push("accepted");
  const pending = assert.rejects(
    queue.push("pending"),
    BoundedQueueClosedError,
  );
  queue.close();
  queue.close();
  await pending;
  assert.equal(await queue.next(), "accepted");
  await assert.rejects(queue.next(), BoundedQueueClosedError);
  await assert.rejects(queue.push("late"), BoundedQueueClosedError);

  const idle = createBoundedQueue(1);
  const consumer = assert.rejects(idle.next(), BoundedQueueClosedError);
  idle.close();
  await consumer;
});

test("bounded processor completes accepted messages on shutdown", async (t) => {
  t.mock.method(console, "error", () => {});
  const processed = [];
  const processor = createBoundedProcessor(2, 1, async (message) => {
    await sleep(1);
    processed.push(message);
    if (message === "B") throw new Error("failed");
  });
  for (const message of ["A", "B", "C", "D", "E"]) {
    await processor.push(message);
  }
  await processor.shutdown();
  await processor.shutdown();
  assert.deepEqual(processed.sort(), ["A", "B", "C", "D", "E"]);
  await assert.rejects(processor.push("late"), BoundedQueueClosedError);
});

test("invalid limits and callbacks fail before work starts", async () => {
  for (const invalid of [0, -1, 1.5, NaN, Infinity]) {
    assert.throws(() => createRateLimiter(invalid, 10), RangeError);
    assert.throws(() => createLRUCache(invalid), RangeError);
    assert.throws(() => createBatcher(invalid, 10), RangeError);
    assert.throws(
      () => createCircuitBreaker(() => {}, invalid, 10),
      RangeError,
    );
    assert.throws(() => createMessageProcessor(invalid, () => {}), RangeError);
    assert.throws(() => createGracefulProcessor(invalid, () => {}), RangeError);
    assert.throws(() => createBoundedQueue(invalid), RangeError);
    assert.throws(
      () => createBoundedProcessor(invalid, 1, () => {}),
      RangeError,
    );
  }
  assert.throws(() => debounce(null, 0), TypeError);
  assert.throws(() => debounce(() => {}, -1), RangeError);
  assert.throws(() => createRequestCache(null), TypeError);
  assert.throws(() => createRateLimiter(1, 0), RangeError);
  assert.throws(() => createBatcher(1, -1), RangeError);
  assert.throws(() => createCircuitBreaker(() => {}, 1, -1), RangeError);
  assert.throws(() => createMessageProcessor(1, null), TypeError);
  assert.throws(() => createGracefulProcessor(1, null), TypeError);
  assert.throws(() => createBoundedProcessor(1, 1, null), TypeError);
  assert.throws(() => createBoundedQueue(1, -1), RangeError);
  assert.throws(() => createEventEmitter().on("event", null), TypeError);
  assert.throws(() => createEventEmitter().once("event", null), TypeError);
  await assert.rejects(retry(null, 1, 0), TypeError);
  await assert.rejects(
    retry(() => {}, 1, -1),
    RangeError,
  );
  await assert.rejects(withTimeout(null, 1), TypeError);
  await assert.rejects(
    withTimeout(() => {}, -1),
    RangeError,
  );
});
