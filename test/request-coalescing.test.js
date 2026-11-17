import assert from "node:assert/strict";
import { getEventListeners } from "node:events";
import { setImmediate as nextTurn } from "node:timers/promises";
import test from "node:test";
import { createRequestCoalescer } from "../request-coalescing.js";

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((onResolve, onReject) => {
    resolve = onResolve;
    reject = onReject;
  });
  return { promise, resolve, reject };
}

test("coalescer shares only in-flight work and gives each caller its own promise", async () => {
  let calls = 0;
  const coalescer = createRequestCoalescer(() => ++calls);
  const first = coalescer.request("route");
  const second = coalescer.request("route");
  assert.notEqual(first, second);
  assert.deepEqual(await Promise.all([first, second]), [1, 1]);
  assert.equal(await coalescer.request("route"), 2);
});

test("coalescer keeps different request keys independent", async () => {
  const calls = [];
  const coalescer = createRequestCoalescer((key) => {
    calls.push(key);
    return key;
  });
  assert.deepEqual(
    await Promise.all([
      coalescer.request("__proto__"),
      coalescer.request("another"),
      coalescer.request("__proto__"),
    ]),
    ["__proto__", "another", "__proto__"],
  );
  assert.deepEqual(calls, ["__proto__", "another"]);
});

test("one caller can cancel without aborting work still needed by another", async () => {
  const pending = deferred();
  let sharedSignal;
  const coalescer = createRequestCoalescer((key, { signal }) => {
    sharedSignal = signal;
    return pending.promise;
  });
  const callerA = new AbortController();
  const callerB = new AbortController();
  const reason = new Error("caller A cancelled");
  const cancelled = assert.rejects(
    coalescer.request("route", callerA.signal),
    (error) => error === reason,
  );
  const remaining = coalescer.request("route", callerB.signal);
  await nextTurn();
  callerA.abort(reason);
  await cancelled;
  assert.equal(sharedSignal.aborted, false);
  assert.equal(getEventListeners(callerA.signal, "abort").length, 0);
  pending.resolve("flights");
  assert.equal(await remaining, "flights");
  assert.equal(getEventListeners(callerB.signal, "abort").length, 0);
  callerB.abort();
  assert.equal(sharedSignal.aborted, false);
});

test("last cancellation aborts shared work and late cleanup cannot delete its replacement", async () => {
  const calls = [];
  const coalescer = createRequestCoalescer((key, { signal }) => {
    const pending = deferred();
    calls.push({ ...pending, signal });
    return pending.promise;
  });
  const first = new AbortController();
  const second = new AbortController();
  const oldResults = Promise.allSettled([
    coalescer.request("route", first.signal),
    coalescer.request("route", second.signal),
  ]);
  await nextTurn();
  first.abort();
  assert.equal(calls[0].signal.aborted, false);
  second.abort();
  assert.equal(calls[0].signal.aborted, true);
  assert.ok((await oldResults).every((result) => result.status === "rejected"));

  const replacement = coalescer.request("route");
  await nextTurn();
  calls[0].reject(new Error("late abort response"));
  await nextTurn();
  const joined = coalescer.request("route");
  await nextTurn();
  assert.equal(calls.length, 2);
  calls[1].resolve("fresh");
  assert.deepEqual(await Promise.all([replacement, joined]), [
    "fresh",
    "fresh",
  ]);
});

test("already cancelled callers never start or join an API request", async () => {
  let calls = 0;
  const coalescer = createRequestCoalescer(() => ++calls);
  const caller = new AbortController();
  caller.abort();
  await assert.rejects(coalescer.request("route", caller.signal), {
    name: "AbortError",
  });
  assert.equal(calls, 0);
  assert.equal(await coalescer.request("route"), 1);
});

test("cancelling all callers before the API starts prevents the operation", async () => {
  let calls = 0;
  const coalescer = createRequestCoalescer(() => ++calls);
  const caller = new AbortController();
  const cancelled = assert.rejects(coalescer.request("route", caller.signal), {
    name: "AbortError",
  });
  caller.abort();
  await cancelled;
  await nextTurn();
  assert.equal(calls, 0);
  assert.equal(await coalescer.request("route"), 1);
});

test("API failures reach every caller, remove listeners, and allow a fresh attempt", async () => {
  for (const asynchronous of [false, true]) {
    let calls = 0;
    const failure = new Error("offline");
    const coalescer = createRequestCoalescer(() => {
      if (++calls === 1) {
        if (asynchronous) return Promise.reject(failure);
        throw failure;
      }
      return "recovered";
    });
    const caller = new AbortController();
    const results = await Promise.allSettled([
      coalescer.request("route", caller.signal),
      coalescer.request("route"),
    ]);
    assert.ok(
      results.every(
        (result) => result.status === "rejected" && result.reason === failure,
      ),
    );
    assert.equal(getEventListeners(caller.signal, "abort").length, 0);
    assert.equal(await coalescer.request("route"), "recovered");
  }
});

test("invalid coalescer inputs fail before starting an API call", async () => {
  assert.throws(() => createRequestCoalescer(null), TypeError);
  let calls = 0;
  const coalescer = createRequestCoalescer(() => calls++);
  await assert.rejects(coalescer.request("route", {}), TypeError);
  assert.equal(calls, 0);
});
