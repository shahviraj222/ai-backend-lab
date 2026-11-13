import { pathToFileURL } from "node:url";

export function createRateLimiter(maxRequests, windowMs) {
  if (!Number.isInteger(maxRequests) || maxRequests < 1) {
    throw new RangeError("Maximum requests must be a positive integer");
  }
  if (!Number.isFinite(windowMs) || windowMs <= 0) {
    throw new RangeError("Window must be a positive number");
  }

  const users = new Map();

  return function allowRequest(user) {
    const now = performance.now();

    for (const [key, timestamps] of users) {
      while (timestamps.length > 0 && now - timestamps[0] >= windowMs) {
        timestamps.shift();
      }
      if (timestamps.length === 0) users.delete(key);
    }

    const timestamps = users.get(user) ?? [];
    if (timestamps.length >= maxRequests) return false;

    timestamps.push(now);
    users.set(user, timestamps);
    return true;
  };
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const allowRequest = createRateLimiter(3, 10_000);

  for (let i = 0; i < 4; i++) {
    console.log("user101:", allowRequest("user101"));
  }
  console.log("user202:", allowRequest("user202"));
}
