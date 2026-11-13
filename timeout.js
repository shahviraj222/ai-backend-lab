import { pathToFileURL } from "node:url";

export async function withTimeout(fn, timeoutMs) {
  if (typeof fn !== "function") {
    throw new TypeError("Operation must be a function");
  }
  if (!Number.isFinite(timeoutMs) || timeoutMs < 0) {
    throw new RangeError("Timeout must be a non-negative number");
  }

  let timer;
  const timeout = new Promise((resolve, reject) => {
    timer = setTimeout(() => reject(new Error("API timeout")), timeoutMs);
  });

  try {
    return await Promise.race([Promise.resolve().then(fn), timeout]);
  } finally {
    clearTimeout(timer);
  }
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const fetchData = () =>
    new Promise((resolve) => setTimeout(() => resolve("API success"), 200));

  console.log(await withTimeout(fetchData, 500));
}
