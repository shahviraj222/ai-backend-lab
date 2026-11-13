import { pathToFileURL } from "node:url";

export function debounce(callback, delay) {
  if (typeof callback !== "function") {
    throw new TypeError("Callback must be a function");
  }
  if (!Number.isFinite(delay) || delay < 0) {
    throw new RangeError("Delay must be a non-negative number");
  }

  let timer;

  return function (...args) {
    clearTimeout(timer);
    timer = setTimeout(() => {
      timer = undefined;
      callback.apply(this, args);
    }, delay);
  };
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const search = debounce((text) => console.log("API call:", text), 300);

  for (const text of ["V", "Vi", "Vir", "Vira", "Viraj"]) {
    search(text);
  }
}
