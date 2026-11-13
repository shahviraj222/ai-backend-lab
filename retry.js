import { pathToFileURL } from "node:url";

export async function retry(fn, maxAttempts, delay) {
  if (typeof fn !== "function") {
    throw new TypeError("Operation must be a function");
  }
  if (!Number.isInteger(maxAttempts) || maxAttempts < 1) {
    throw new RangeError("Maximum attempts must be a positive integer");
  }
  if (!Number.isFinite(delay) || delay < 0) {
    throw new RangeError("Delay must be a non-negative number");
  }

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await fn();
    } catch (error) {
      if (attempt === maxAttempts) throw error;
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  let attempts = 0;
  const unstableAPI = async () => {
    attempts++;
    console.log("Attempt:", attempts);
    if (attempts < 3) throw new Error("Temporary network error");
    return "API success";
  };

  console.log(await retry(unstableAPI, 3, 200));
}
