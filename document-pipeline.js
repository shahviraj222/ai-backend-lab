import { randomUUID } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { pathToFileURL } from "node:url";

function createAsyncQueue() {
  const messages = [];
  const waitingConsumers = [];

  return {
    push(item) {
      if (waitingConsumers.length > 0) waitingConsumers.shift()(item);
      else messages.push(item);
    },

    next() {
      if (messages.length > 0) return Promise.resolve(messages.shift());
      return new Promise((resolve) => waitingConsumers.push(resolve));
    },
  };
}

export function createDocumentPipeline({
  extractText,
  summarizeText,
  saveDocument,
  concurrency = {},
} = {}) {
  if (
    [extractText, summarizeText, saveDocument].some(
      (handler) => typeof handler !== "function",
    )
  ) {
    throw new TypeError("Each pipeline stage must provide a function");
  }

  const limits = {
    extraction: 2,
    summarization: 1,
    database: 2,
    ...concurrency,
  };
  for (const stage of ["extraction", "summarization", "database"]) {
    if (!Number.isInteger(limits[stage]) || limits[stage] < 1) {
      throw new RangeError(`${stage} concurrency must be a positive integer`);
    }
  }

  const extractionQueue = createAsyncQueue();
  const summaryQueue = createAsyncQueue();
  const databaseQueue = createAsyncQueue();
  const jobs = new Map();

  function getStatus(id) {
    const job = jobs.get(id);
    if (!job) return null;

    return {
      id: job.id,
      pdfUrl: job.pdfUrl,
      status: job.status,
      stage: job.stage,
      error: job.error,
    };
  }

  function markFailed(job, error) {
    job.status = "failed";
    job.error = error instanceof Error ? error.message : String(error);
    job.resolveDone(getStatus(job.id));
  }

  async function extractionWorker() {
    while (true) {
      const job = await extractionQueue.next();

      try {
        job.status = "processing";
        job.stage = "extraction";
        job.extractedText = await extractText(job.pdfUrl);
        summaryQueue.push(job);
      } catch (error) {
        markFailed(job, error);
      }
    }
  }

  async function summaryWorker() {
    while (true) {
      const job = await summaryQueue.next();

      try {
        job.stage = "summarization";
        job.summary = await summarizeText(job.extractedText);
        databaseQueue.push(job);
      } catch (error) {
        markFailed(job, error);
      }
    }
  }

  async function databaseWorker() {
    while (true) {
      const job = await databaseQueue.next();

      try {
        job.stage = "database";
        await saveDocument({
          id: job.id,
          pdfUrl: job.pdfUrl,
          extractedText: job.extractedText,
          summary: job.summary,
        });
        job.status = "completed";
        job.stage = "finished";
        job.resolveDone(getStatus(job.id));
      } catch (error) {
        markFailed(job, error);
      }
    }
  }

  for (let i = 0; i < limits.extraction; i++) void extractionWorker();
  for (let i = 0; i < limits.summarization; i++) void summaryWorker();
  for (let i = 0; i < limits.database; i++) void databaseWorker();

  return {
    push(pdfUrl) {
      if (typeof pdfUrl !== "string" || pdfUrl.trim() === "") {
        throw new TypeError("Document location must be a non-empty string");
      }

      const id = randomUUID();
      let resolveDone;
      const done = new Promise((resolve) => {
        resolveDone = resolve;
      });
      const job = {
        id,
        pdfUrl,
        extractedText: null,
        summary: null,
        status: "pending",
        stage: "queued",
        error: null,
        resolveDone,
      };

      jobs.set(id, job);
      extractionQueue.push(job);
      return { id, done };
    },
    getStatus,
  };
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const database = new Map();
  const pipeline = createDocumentPipeline({
    async extractText(pdfUrl) {
      await sleep(100);
      console.log("Extracted:", pdfUrl);
      return `Content from ${pdfUrl}`;
    },
    async summarizeText(text) {
      await sleep(150);
      if (text.includes("bad-document"))
        throw new Error("AI summarization failed");
      return `Summary of: ${text}`;
    },
    async saveDocument(record) {
      await sleep(80);
      database.set(record.id, record);
      console.log("Saved:", record.pdfUrl);
    },
  });

  const jobs = ["document-A", "document-B", "bad-document"].map((name) =>
    pipeline.push(`https://storage.example.com/${name}.pdf`),
  );
  console.log("Initial status:", pipeline.getStatus(jobs[0].id));
  console.log("Final results:", await Promise.all(jobs.map((job) => job.done)));
  console.log("Saved records:", database.size);
}
