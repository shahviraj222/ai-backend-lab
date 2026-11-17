import { pathToFileURL } from "node:url";

export function createAsyncQueue() {
  const messages = [];
  const waitingConsumers = [];

  return {
    push(item) {
      if (waitingConsumers.length > 0) {
        const resolve = waitingConsumers.shift();
        resolve(item);
      } else {
        messages.push(item);
      }
    },

    next() {
      if (messages.length > 0) return Promise.resolve(messages.shift());

      return new Promise((resolve) => {
        waitingConsumers.push(resolve);
      });
    },
  };
}

export function createMessageListener(queue) {
  if (typeof queue?.next !== "function") {
    throw new TypeError("Queue must provide a next function");
  }

  const listeners = new Set();

  async function listen() {
    while (true) {
      const message = await queue.next();

      for (const callback of [...listeners]) {
        try {
          await callback(message);
        } catch (error) {
          console.error("Message listener failed:", error);
        }
      }
    }
  }

  void listen().catch((error) =>
    console.error("Queue listener stopped:", error),
  );

  return {
    onMessage(callback) {
      if (typeof callback !== "function") {
        throw new TypeError("Listener must be a function");
      }

      listeners.add(callback);
      return () => listeners.delete(callback);
    },
  };
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const queue = createAsyncQueue();
  const pendingMessage = queue.next();
  queue.push("A");
  queue.push("B");
  queue.push("C");

  console.log(await pendingMessage);
  console.log(await queue.next());
  console.log(await queue.next());

  const liveQueue = createAsyncQueue();
  const bot = createMessageListener(liveQueue);
  bot.onMessage((message) => console.log("Message received:", message));
  bot.onMessage((message) => console.log("Message length:", message.length));
  liveQueue.push("Hello from Viraj");
}
