import { setTimeout as sleep } from "node:timers/promises";
import { pathToFileURL } from "node:url";

export function createRequestCoalescer(apiFunction) {
  if (typeof apiFunction !== "function") {
    throw new TypeError("API operation must be a function");
  }

  const registry = new Map();

  function request(key, callerSignal) {
    if (callerSignal != null && !(callerSignal instanceof AbortSignal)) {
      return Promise.reject(
        new TypeError("Caller signal must be an AbortSignal"),
      );
    }
    if (callerSignal?.aborted) return Promise.reject(callerSignal.reason);

    let entry = registry.get(key);

    if (!entry) {
      const controller = new AbortController();
      entry = { controller, sharedPromise: null, callers: new Set() };
      registry.set(key, entry);

      entry.sharedPromise = Promise.resolve().then(() => {
        controller.signal.throwIfAborted();
        return apiFunction(key, { signal: controller.signal });
      });

      function removeEntry() {
        // A cancelled request may settle after a newer request has started.
        if (registry.get(key) === entry) registry.delete(key);
      }

      entry.sharedPromise.then(removeEntry, removeEntry);
    }

    return new Promise((resolve, reject) => {
      const caller = {};
      entry.callers.add(caller);
      let settled = false;

      function finish(success, value) {
        if (settled) return;
        settled = true;
        entry.callers.delete(caller);
        callerSignal?.removeEventListener("abort", onAbort);

        if (success) resolve(value);
        else reject(value);
      }

      function onAbort() {
        finish(false, callerSignal.reason);

        if (entry.callers.size === 0 && registry.get(key) === entry) {
          registry.delete(key);
          entry.controller.abort(callerSignal.reason);
        }
      }

      callerSignal?.addEventListener("abort", onAbort, { once: true });
      if (callerSignal?.aborted) {
        onAbort();
        return;
      }

      entry.sharedPromise.then(
        (result) => finish(true, result),
        (error) => finish(false, error),
      );
    });
  }

  return { request };
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const coalescer = createRequestCoalescer(async (route, { signal }) => {
    console.log("API called:", route);
    await sleep(200, undefined, { signal });
    return `Flights found for ${route}`;
  });

  const callerA = new AbortController();
  const requests = [
    coalescer.request("Mumbai-LA", callerA.signal),
    coalescer.request("Mumbai-LA"),
    coalescer.request("Mumbai-LA"),
  ];
  setTimeout(() => callerA.abort(new Error("User A cancelled")), 50);

  for (const [index, result] of (
    await Promise.allSettled(requests)
  ).entries()) {
    console.log(
      `User ${index + 1}:`,
      result.status === "fulfilled" ? result.value : result.reason.message,
    );
  }

  const callerB = new AbortController();
  const callerC = new AbortController();
  const cancelledRequests = [
    coalescer.request("Mumbai-Goa", callerB.signal),
    coalescer.request("Mumbai-Goa", callerC.signal),
  ];
  setTimeout(() => {
    callerB.abort(new Error("User B cancelled"));
    callerC.abort(new Error("User C cancelled"));
  }, 50);

  console.log(
    "All callers cancelled:",
    (await Promise.allSettled(cancelledRequests)).map(
      (result) => result.status,
    ),
  );
}
