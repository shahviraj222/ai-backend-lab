import { pathToFileURL } from "node:url";
function createAbortError() {
  const error = new Error("Request cancelled by user");
  error.name = "AbortError";
  return error;
}

function delay(ms, signal) {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(createAbortError());
      return;
    }

    const timer = setTimeout(() => {
      cleanup();
      resolve();
    }, ms);

    function cleanup() {
      signal?.removeEventListener("abort", onAbort);
    }

    function onAbort() {
      clearTimeout(timer);
      cleanup();
      reject(createAbortError());
    }

    signal?.addEventListener("abort", onAbort, {
      once: true,
    });

    if (signal?.aborted) {
      onAbort();
    }
  });
}

function fakeAIRequest(prompt, { signal }) {
  return new Promise((resolve, reject) => {
    console.log("API request started:", prompt);

    if (signal.aborted) {
      reject(new Error("Request aborted"));
      return;
    }

    const timer = setTimeout(() => {
      cleanup();
      resolve(`AI response for: ${prompt}`);
    }, 100);

    function onAbort() {
      clearTimeout(timer);
      cleanup();
      reject(new Error("Request aborted"));
    }

    function cleanup() {
      signal.removeEventListener("abort", onAbort);
    }

    signal.addEventListener("abort", onAbort, {
      once: true,
    });
  });
}

export async function requestWithRetry(apiFunction, input, options = {}) {
  if (typeof apiFunction !== "function")
    throw new TypeError("API operation must be a function");
  const { timeout = 2000, maxRetries = 3, baseDelay = 1000, signal } = options;

  if (
    !Number.isFinite(timeout) ||
    timeout < 0 ||
    !Number.isFinite(baseDelay) ||
    baseDelay < 0 ||
    !Number.isInteger(maxRetries) ||
    maxRetries < 0
  ) {
    throw new Error("Invalid retry configuration");
  }

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    if (signal?.aborted) {
      throw createAbortError();
    }

    const controller = new AbortController();

    let timeoutId;
    let onUserAbort;

    const abortPromise = new Promise((_, reject) => {
      onUserAbort = () => {
        controller.abort();
        reject(createAbortError());
      };

      if (signal?.aborted) {
        onUserAbort();
        return;
      }

      signal?.addEventListener("abort", onUserAbort, {
        once: true,
      });
    });

    try {
      console.log(`Starting attempt ${attempt + 1}`);

      const apiCall = Promise.resolve().then(() => {
        if (controller.signal.aborted) throw createAbortError();
        return apiFunction(input, {
          signal: controller.signal,
        });
      });

      const timeoutPromise = new Promise((resolve) => {
        timeoutId = setTimeout(() => {
          resolve({
            source: "timeout",
          });
        }, timeout);
      });

      const result = await Promise.race([
        apiCall.then((data) => {
          return {
            source: "api",
            value: data,
          };
        }),

        timeoutPromise,

        abortPromise,
      ]);

      if (signal?.aborted) {
        throw createAbortError();
      }

      if (result.source === "timeout") {
        controller.abort();

        const error = new Error(`Request timed out after ${timeout}ms`);

        error.name = "TimeoutError";
        throw error;
      }

      console.log(`Attempt ${attempt + 1} succeeded`);

      return result.value;
    } catch (error) {
      if (signal?.aborted) {
        throw createAbortError();
      }

      console.log(
        `Attempt ${attempt + 1} failed:`,
        error instanceof Error ? error.message : String(error),
      );

      if (attempt === maxRetries) {
        throw error;
      }
    } finally {
      clearTimeout(timeoutId);

      signal?.removeEventListener("abort", onUserAbort);
    }

    const retryDelay = Math.min(2147483647, baseDelay * 2 ** attempt);

    console.log(`Retrying after ${retryDelay}ms`);

    await delay(retryDelay, signal);
  }
}

async function main() {
  console.log("Test 1: API timeout and retries");

  try {
    const result = await requestWithRetry(
      fakeAIRequest,
      "Explain JavaScript Promises",
      {
        timeout: 30,
        maxRetries: 3,
        baseDelay: 10,
      },
    );

    console.log("Result:", result);
  } catch (error) {
    console.log(
      "Final error:",
      error instanceof Error ? error.message : String(error),
    );
  }

  console.log("\nTest 2: Successful API request");

  try {
    const result = await requestWithRetry(
      fakeAIRequest,
      "Explain async/await",
      {
        timeout: 300,
        maxRetries: 3,
        baseDelay: 10,
      },
    );

    console.log("Result:", result);
  } catch (error) {
    console.log(
      "Final error:",
      error instanceof Error ? error.message : String(error),
    );
  }

  console.log("\nTest 3: User cancellation");

  const controller = new AbortController();

  setTimeout(() => {
    console.log("User cancelled the request");
    controller.abort();
  }, 20);

  try {
    const result = await requestWithRetry(
      fakeAIRequest,
      "Explain AbortController",
      {
        timeout: 30,
        maxRetries: 3,
        baseDelay: 10,
        signal: controller.signal,
      },
    );

    console.log("Result:", result);
  } catch (error) {
    console.log(
      "Final error:",
      error instanceof Error ? error.message : String(error),
    );
  }
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  await main();
}
