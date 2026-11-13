import { pathToFileURL } from "node:url";

export function createBatcher(
  batchSize,
  maxWaitMs,
  sendBatch = async (events) => console.log("Sending:", events),
  onError = (error) => console.error("Batch failed:", error),
) {
  if (!Number.isInteger(batchSize) || batchSize < 1) {
    throw new RangeError("Batch size must be a positive integer");
  }
  if (!Number.isFinite(maxWaitMs) || maxWaitMs < 0) {
    throw new RangeError("Maximum wait must be a non-negative number");
  }
  if (typeof sendBatch !== "function" || typeof onError !== "function") {
    throw new TypeError("Batch sender and error handler must be functions");
  }

  let events = [];
  let timer = null;

  async function flush() {
    clearTimeout(timer);
    timer = null;
    if (events.length === 0) return;

    const batch = events;
    events = [];
    await sendBatch(batch);
  }

  function flushAutomatically() {
    void flush().catch(onError);
  }

  return {
    add(event) {
      events.push(event);

      if (events.length >= batchSize) {
        flushAutomatically();
      } else if (timer === null) {
        timer = setTimeout(flushAutomatically, maxWaitMs);
      }
    },
    flush,
  };
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const batcher = createBatcher(3, 300);
  for (const event of ["A", "B", "C", "D", "E"]) {
    batcher.add(event);
  }
}
