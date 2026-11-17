import assert from "node:assert/strict";
import { setImmediate as nextTurn } from "node:timers/promises";
import test from "node:test";
import { createAsyncQueue, createMessageListener } from "../async-queue.js";

test("queue listener broadcasts messages in order and supports unsubscribe", async () => {
  const queue = createAsyncQueue();
  const bot = createMessageListener(queue);
  const received = [];
  const unsubscribe = bot.onMessage((message) =>
    received.push(["first", message]),
  );
  bot.onMessage((message) => received.push(["second", message]));

  queue.push("A");
  queue.push("B");
  await nextTurn();
  unsubscribe();
  queue.push("C");
  await nextTurn();

  assert.deepEqual(received, [
    ["first", "A"],
    ["second", "A"],
    ["first", "B"],
    ["second", "B"],
    ["second", "C"],
  ]);
});

test("listener changes take effect on the next message", async () => {
  const queue = createAsyncQueue();
  const bot = createMessageListener(queue);
  const received = [];
  bot.onMessage(() => unsubscribe());
  const unsubscribe = bot.onMessage((message) => received.push(message));
  queue.push("A");
  queue.push("B");
  await nextTurn();
  assert.deepEqual(received, ["A"]);
});

test("throwing and rejecting listeners do not stop later listeners or messages", async (t) => {
  const errors = t.mock.method(console, "error", () => {});
  const queue = createAsyncQueue();
  const bot = createMessageListener(queue);
  const received = [];
  bot.onMessage(() => {
    throw new Error("synchronous failure");
  });
  bot.onMessage(async () => {
    throw new Error("asynchronous failure");
  });
  bot.onMessage((message) => received.push(message));
  queue.push("A");
  queue.push("B");
  await nextTurn();
  assert.deepEqual(received, ["A", "B"]);
  assert.equal(errors.mock.callCount(), 4);
});

test("listener validates its inputs and reports a queue failure", async (t) => {
  assert.throws(() => createMessageListener(null), TypeError);
  const bot = createMessageListener(createAsyncQueue());
  assert.throws(() => bot.onMessage(null), TypeError);

  const errors = t.mock.method(console, "error", () => {});
  const failure = new Error("queue unavailable");
  createMessageListener({ next: () => Promise.reject(failure) });
  await nextTurn();
  assert.equal(errors.mock.calls[0].arguments[1], failure);
});
