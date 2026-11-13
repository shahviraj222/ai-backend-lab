import { pathToFileURL } from "node:url";

export function createRequestCache(fetchData) {
  if (typeof fetchData !== "function") {
    throw new TypeError("fetchData must be a function");
  }

  const requests = new Map();

  return function getData(key) {
    if (requests.has(key)) {
      return requests.get(key);
    }

    const request = Promise.resolve()
      .then(() => fetchData(key))
      .catch((error) => {
        requests.delete(key);
        throw error;
      });

    requests.set(key, request);
    return request;
  };
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const getUser = createRequestCache(async (id) => {
    console.log("API request started:", id);
    await new Promise((resolve) => setTimeout(resolve, 300));
    return { id, name: "Viraj" };
  });

  console.log(await Promise.all([getUser(101), getUser(101), getUser(101)]));
}
