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
}
