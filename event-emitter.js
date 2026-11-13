import { pathToFileURL } from "node:url";

export function createEventEmitter() {
  const registry = new Map();

  function on(name, callback) {
    if (typeof callback !== "function") {
      throw new TypeError("Listener must be a function");
    }
    const listeners = registry.get(name) ?? [];
    listeners.push(callback);
    registry.set(name, listeners);
  }

  function off(name, callback) {
    const listeners = registry.get(name);
    if (!listeners) return;

    const remaining = listeners.filter((listener) => listener !== callback);
    if (remaining.length === 0) {
      registry.delete(name);
    } else {
      registry.set(name, remaining);
    }
  }

  function emit(name, ...args) {
    // A snapshot keeps listener removal from skipping the next callback.
    for (const callback of [...(registry.get(name) ?? [])]) {
      callback(...args);
    }
  }

  function once(name, callback) {
    if (typeof callback !== "function") {
      throw new TypeError("Listener must be a function");
    }
    let called = false;

    function wrapper(...args) {
      if (called) return;
      called = true;
      off(name, wrapper);
      callback(...args);
    }

    on(name, wrapper);
  }

  return { on, off, emit, once };
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const events = createEventEmitter();
  const logMessage = (message) => console.log("Message:", message);

  events.on("message", logMessage);
  events.once("message", () => console.log("First message received"));
  events.emit("message", "Hello");
  events.emit("message", "How can I help?");
  events.off("message", logMessage);
  events.emit("message", "No listeners remain");
}
