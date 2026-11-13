import { pathToFileURL } from "node:url";

export function createCircuitBreaker(fn, maxFailures, cooldownMs) {
  if (typeof fn !== "function") {
    throw new TypeError("Operation must be a function");
  }
  if (!Number.isInteger(maxFailures) || maxFailures < 1) {
    throw new RangeError("Maximum failures must be a positive integer");
  }
  if (!Number.isFinite(cooldownMs) || cooldownMs < 0) {
    throw new RangeError("Cooldown must be a non-negative number");
  }

  let failureCount = 0;
  let state = "CLOSED";
  let retryAt = 0;
  let testInProgress = false;
  let generation = 0;

  function openCircuit() {
    state = "OPEN";
    retryAt = performance.now() + cooldownMs;
    generation++;
  }

  return async function (...args) {
    if (state === "OPEN") {
      if (performance.now() < retryAt) {
        throw new Error("Circuit is OPEN. Request blocked.");
      }
      state = "HALF_OPEN";
    }

    if (state === "HALF_OPEN") {
      if (testInProgress) throw new Error("Test request already running.");
      testInProgress = true;

      try {
        const data = await fn.apply(this, args);
        state = "CLOSED";
        failureCount = 0;
        generation++;
        return data;
      } catch (error) {
        openCircuit();
        throw error;
      } finally {
        testInProgress = false;
      }
    }

    // Requests from before an outage must not change the recovered circuit.
    const requestGeneration = generation;

    try {
      const data = await fn.apply(this, args);
      if (state === "CLOSED" && generation === requestGeneration) {
        failureCount = 0;
      }
      return data;
    } catch (error) {
      if (state === "CLOSED" && generation === requestGeneration) {
        failureCount++;
        if (failureCount >= maxFailures) openCircuit();
      }
      throw error;
    }
  };
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  let available = false;
  const callAPI = createCircuitBreaker(
    async () => {
      if (!available) throw new Error("Service unavailable");
      return "Service recovered";
    },
    2,
    200,
  );

  for (let i = 0; i < 3; i++) {
    try {
      await callAPI();
    } catch (error) {
      console.log(error.message);
    }
  }

  available = true;
  await new Promise((resolve) => setTimeout(resolve, 250));
  console.log(await callAPI());
}
