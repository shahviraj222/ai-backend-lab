import { pathToFileURL } from "node:url";

export function createLRUCache(capacity) {
  if (!Number.isInteger(capacity) || capacity < 1) {
    throw new RangeError("Capacity must be a positive integer");
  }

  const entries = [];

  function findIndex(key) {
    return entries.findIndex((entry) => Object.is(entry.key, key));
  }

  function moveToFront(index) {
    const [entry] = entries.splice(index, 1);
    entries.unshift(entry);
  }

  return {
    get(key) {
      const index = findIndex(key);
      if (index === -1) return -1;

      const value = entries[index].value;
      moveToFront(index);
      return value;
    },

    put(key, value) {
      const index = findIndex(key);
      if (index !== -1) {
        entries[index].value = value;
        moveToFront(index);
        return;
      }

      entries.unshift({ key, value });
      if (entries.length > capacity) entries.pop();
    },
  };
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const cache = createLRUCache(3);
  cache.put("A", 100);
  cache.put("B", 200);
  cache.put("C", 300);

  console.log("Read A:", cache.get("A"));
  cache.put("D", 400);
  console.log("B after eviction:", cache.get("B"));
  console.log("Read D:", cache.get("D"));
}
