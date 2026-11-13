import { pathToFileURL } from "node:url";

export async function runTasks(tasks, limit) {
  if (
    !Array.isArray(tasks) ||
    !tasks.every((task) => typeof task === "function")
  ) {
    throw new TypeError("Tasks must be an array of functions");
  }
  if (!Number.isInteger(limit) || limit < 1) {
    throw new RangeError("Concurrency limit must be a positive integer");
  }

  const results = new Array(tasks.length);
  let nextIndex = 0;

  async function startWorker() {
    while (nextIndex < tasks.length) {
      const index = nextIndex++;

      try {
        results[index] = await tasks[index]();
      } catch (error) {
        results[index] = {
          success: false,
          message: error instanceof Error ? error.message : String(error),
        };
      }
    }
  }

  const workers = [];
  for (let i = 0; i < Math.min(limit, tasks.length); i++) {
    workers.push(startWorker());
  }
  await Promise.all(workers);

  return results;
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const fetchData = (name, delay) =>
    new Promise((resolve) => {
      setTimeout(() => {
        console.log(`${name} completed`);
        resolve(name);
      }, delay);
    });

  const tasks = [
    () => fetchData("User 1", 300),
    () => fetchData("User 2", 100),
    () => fetchData("User 3", 200),
    () => fetchData("User 4", 100),
    () => fetchData("User 5", 200),
  ];

  console.log(await runTasks(tasks, 2));
}
