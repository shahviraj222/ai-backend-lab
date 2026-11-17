import assert from "node:assert/strict";
import { setImmediate as nextTurn } from "node:timers/promises";
import test from "node:test";
import { createDocumentPipeline } from "../document-pipeline.js";

function deferred() {
  let resolve;
  const promise = new Promise((onResolve) => {
    resolve = onResolve;
  });
  return { promise, resolve };
}

function handlers() {
  return {
    extractText: (url) => `Text: ${url}`,
    summarizeText: (text) => `Summary: ${text}`,
    saveDocument: (record) => record.id,
  };
}

const stageHandlers = {
  extraction: "extractText",
  summarization: "summarizeText",
  database: "saveDocument",
};

test("pipeline passes each stage's output forward and returns independent status snapshots", async () => {
  const saved = [];
  const pipeline = createDocumentPipeline({
    ...handlers(),
    saveDocument: (record) => saved.push(record),
  });
  const job = pipeline.push("document.pdf");
  const initial = pipeline.getStatus(job.id);
  assert.equal(initial.status, "pending");
  initial.status = "modified by caller";
  assert.equal(pipeline.getStatus(job.id).status, "pending");
  assert.equal(pipeline.getStatus("missing"), null);

  assert.deepEqual(await job.done, {
    id: job.id,
    pdfUrl: "document.pdf",
    status: "completed",
    stage: "finished",
    error: null,
  });
  assert.deepEqual(saved, [
    {
      id: job.id,
      pdfUrl: "document.pdf",
      extractedText: "Text: document.pdf",
      summary: "Summary: Text: document.pdf",
    },
  ]);
});

for (const [stage, handler] of Object.entries(stageHandlers)) {
  test(`pipeline enforces the ${stage} worker limit`, async () => {
    const operations = handlers();
    const original = operations[handler];
    const gate = deferred();
    let active = 0;
    let peak = 0;
    operations[handler] = async (value) => {
      active++;
      peak = Math.max(peak, active);
      await gate.promise;
      active--;
      return original(value);
    };
    const pipeline = createDocumentPipeline({
      ...operations,
      concurrency: { [stage]: 2 },
    });
    const jobs = Array.from({ length: 5 }, (_, index) =>
      pipeline.push(`document-${index}.pdf`),
    );
    await nextTurn();
    assert.equal(active, 2);
    gate.resolve();
    const results = await Promise.all(jobs.map((job) => job.done));
    assert.ok(results.every((result) => result.status === "completed"));
    assert.equal(peak, 2);
    assert.equal(new Set(results.map((result) => result.id)).size, 5);
  });
}

test("different documents can occupy different stages at the same time", async () => {
  const extraction = deferred();
  const summary = deferred();
  let extracting = false;
  let summarizing = false;
  const pipeline = createDocumentPipeline({
    ...handlers(),
    async extractText(url) {
      if (url === "slow.pdf") {
        extracting = true;
        await extraction.promise;
        extracting = false;
      }
      return url;
    },
    async summarizeText(text) {
      summarizing = true;
      await summary.promise;
      summarizing = false;
      return text;
    },
  });
  const slow = pipeline.push("slow.pdf");
  const fast = pipeline.push("fast.pdf");
  await nextTurn();
  assert.equal(extracting, true);
  assert.equal(summarizing, true);
  assert.equal(pipeline.getStatus(slow.id).stage, "extraction");
  assert.equal(pipeline.getStatus(fast.id).stage, "summarization");
  extraction.resolve();
  summary.resolve();
  await Promise.all([slow.done, fast.done]);
});

for (const [stage, handler] of Object.entries(stageHandlers)) {
  test(`failure in ${stage} stops that job without stopping its worker`, async () => {
    const operations = handlers();
    const calls = [];
    for (const [name, operation] of Object.entries(operations)) {
      operations[name] = (value) => {
        const location = typeof value === "string" ? value : value.pdfUrl;
        calls.push([name, location]);
        if (name === handler && location.includes("bad")) {
          if (stage === "summarization") throw "stage failed";
          throw new Error("stage failed");
        }
        return operation(value);
      };
    }
    const pipeline = createDocumentPipeline({
      ...operations,
      concurrency: { extraction: 1, summarization: 1, database: 1 },
    });
    const bad = pipeline.push("bad.pdf");
    const good = pipeline.push("good.pdf");
    const [failure, success] = await Promise.all([bad.done, good.done]);
    assert.equal(failure.status, "failed");
    assert.equal(failure.stage, stage);
    assert.equal(failure.error, "stage failed");
    assert.equal(success.status, "completed");
    const badCalls = calls
      .filter(([, location]) => location.includes("bad"))
      .map(([name]) => name);
    const expected = Object.values(stageHandlers);
    assert.deepEqual(
      badCalls,
      expected.slice(0, expected.indexOf(handler) + 1),
    );
  });
}

test("pipeline validates handlers, stage limits, and document locations", () => {
  assert.throws(() => createDocumentPipeline(), TypeError);
  for (const stage of Object.keys(stageHandlers)) {
    for (const invalid of [0, -1, 1.5, NaN, Infinity]) {
      assert.throws(
        () =>
          createDocumentPipeline({
            ...handlers(),
            concurrency: { [stage]: invalid },
          }),
        RangeError,
      );
    }
  }
  const pipeline = createDocumentPipeline(handlers());
  for (const invalid of [null, 42, "", "  "]) {
    assert.throws(() => pipeline.push(invalid), TypeError);
  }
});
