import { S2 } from "@s2-dev/streamstore";
import { ReadAcceptEnum } from "@s2-dev/streamstore/sdk/records.js";
import { EventStream } from "@s2-dev/streamstore/lib/event-streams.js";
import { ReadBatch, ReadEvent, SequencedRecord } from "@s2-dev/streamstore/models/components";
import { AppendInput, BatchBuilder } from "./batching";
import { FencingToken, SeqNum, TailResponse } from "@s2-dev/streamstore/models/errors";

interface S2Config {
  /**
   * Access token for S2.
   */
  readonly accessToken: string;
  /**
   * Globally unique basin name.
   */
  readonly basin: string;
  /**
   * Number of records to batch together when appending to S2.
   * Defaults to 10 if not set.
   */
  readonly batchSize: number;
  /**
   * Maximum time to wait before flushing a batch (in milliseconds).
   * Defaults to 5000 if not set.
   */
  readonly lingerDuration: number;
}

export interface CreateResumableStreamContextOptions {
  /**
   * A function that takes a promise and ensures that the current program stays alive until the promise is resolved.
   */
  waitUntil: (promise: Promise<unknown>) => void;
}

interface CreateResumableStreamContext {
  waitUntil: (promise: Promise<unknown>) => void;
}

export interface ResumableStreamContext {
  /**
   * Creates a resumable stream from the provided input stream.
   * The stream will be persisted in S2 and can be resumed later.
   * @param streamId - Unique identifier for the stream.
   * @param stream - ReadableStream of data to be persisted.
   * @returns A ReadableStream that can be used to read.
   */
  resumableStream: (
    streamId: string,
    makeStream: () => ReadableStream<string>
  ) => Promise<ReadableStream<string> | null>;
  /**
   * Resumes a previously created stream by its ID.
   * @param streamId - Unique identifier for the stream to resume.
   * @returns A ReadableStream that can be used to read.
   */
  resumeStream: (streamId: string) => Promise<ReadableStream<string> | null>;
  /**
   * Stops a stream by its ID.
   * @param streamId - Unique identifier for the stream to stop.
   */
  stopStream: (streamId: string) => Promise<void>;
}

function generateFencingToken(): string {
  return Math.random().toString(36).slice(2, 7);
}

function getS2Config(): S2Config {
  const accessToken = process.env.S2_ACCESS_TOKEN;
  const basin = process.env.S2_BASIN;
  const batchSize = parseInt(process.env.S2_BATCH_SIZE ?? "10", 10);
  const lingerDuration = parseInt(process.env.S2_LINGER_DURATION ?? "5000", 10);

  if (!accessToken) throw new Error("S2_ACCESS_TOKEN is not set");
  if (!basin) throw new Error("S2_BASIN is not set");

  return { accessToken, basin, batchSize, lingerDuration };
}

export function createResumableStreamContext(
  options: CreateResumableStreamContextOptions
): ResumableStreamContext {
  const ctx = {
    waitUntil: options.waitUntil,
  } as CreateResumableStreamContext;

  getS2Config();
  return {
    resumableStream: async (streamId: string, makeStream: () => ReadableStream<string>) => {
      return await createResumableStream(ctx, makeStream, streamId);
    },
    resumeStream: async (streamId: string) => {
      return await resumeStream(streamId);
    },
    stopStream: async (streamId: string) => {
      return await stopStream(streamId);
    },
  };
}

export async function createResumableStream(
  ctx: CreateResumableStreamContext,
  makeStream: () => ReadableStream<string>,
  streamId: string
): Promise<ReadableStream<string> | null> {
  const { accessToken, basin, batchSize, lingerDuration } = getS2Config();
  const s2 = new S2({ accessToken });
  const [persistentStream, clientStream] = makeStream().tee();
  const sessionFencingToken = "session-" + generateFencingToken();

  const batchBuilder = new BatchBuilder({
    maxBatchRecords: batchSize,
    fencingToken: sessionFencingToken,
  });

  try {
    const lastRecord = (await s2.records.read({
      s2Basin: basin,
      stream: streamId,
      tailOffset: 1,
      count: 1,
    })) as ReadBatch;

    if (isStreamDone(lastRecord)) {
      debugLog("Stream already ended, not resuming:", streamId);
      return null;
    }
  } catch (error: any) {
    if (error instanceof TailResponse) {
      debugLog("Got TailResponse:", error);
    } else {
      debugLog("Error reading last record:", error);
      return null;
    }
  }

  // in case of multiple writers, only one with the given fencing token will succeed
  try {
    await appendFenceCommand(s2, basin, streamId, "", sessionFencingToken);
  } catch (error: any) {
    if (error instanceof FencingToken) {
      debugLog("Stream already exists, resuming existing stream:", streamId, error);
      return await resumeStream(streamId);
    }
    debugLog("Error initializing stream:", error);
    return null;
  }

  const persistStream = async () => {
    const reader = persistentStream.getReader();

    batchBuilder.setMatchSeqNum(1);

    let terminated = false;
    let batchDeadline: Promise<void> | null = null;

    try {
      while (!terminated) {
        while (!batchBuilder.isFull()) {
          if (batchBuilder.hasRecords() && batchDeadline === null) {
            batchDeadline = new Promise((resolve) => setTimeout(resolve, lingerDuration));
          }

          const readPromise = reader.read();
          const promises: Promise<any>[] = [readPromise];

          if (batchDeadline && batchBuilder.hasRecords()) {
            promises.push(batchDeadline);
          }

          const result = await Promise.race(promises);

          if (result === undefined && batchBuilder.hasRecords()) {
            batchDeadline = null;
            break;
          }

          if (result && typeof result === "object" && "done" in result) {
            const { done, value } = result;
            if (done) {
              terminated = true;
              break;
            }

            if (!batchBuilder.addRecord(value)) {
              break;
            }
          }
        }

        if (batchBuilder.hasRecords()) {
          const appendInput = batchBuilder.flush();
          if (appendInput) {
            await appendRecords(s2, basin, streamId, appendInput);
          }
          batchDeadline = null;
        }
      }

      if (batchBuilder.hasRecords()) {
        const appendInput = batchBuilder.flush();
        if (appendInput) {
          await appendRecords(s2, basin, streamId, appendInput);
        }
      }

      await appendFenceCommand(
        s2,
        basin,
        streamId,
        sessionFencingToken,
        "end-" + generateFencingToken()
      );
    } catch (error) {
      debugLog("Error processing stream:", error);
      try {
        await appendFenceCommand(
          s2,
          basin,
          streamId,
          sessionFencingToken,
          "error-" + generateFencingToken()
        );
      } catch (fenceError) {
        debugLog("Error appending fence command:", fenceError);
      }
    } finally {
      reader.releaseLock();
    }
  };

  ctx.waitUntil(persistStream());
  return clientStream;
}

async function resumeStream(streamId: string): Promise<ReadableStream<string> | null> {
  const { accessToken, basin } = getS2Config();
  const s2 = new S2({ accessToken });
  debugLog("Resuming stream:", streamId);
  return new ReadableStream({
    async start(controller) {
      try {
        const events = await s2.records.read(
          {
            s2Basin: basin,
            stream: streamId,
            seqNum: 0,
          },
          {
            acceptHeaderOverride: ReadAcceptEnum.textEventStream,
          }
        );
        const eventsStream = events as EventStream<ReadEvent>;
        await processStream(streamId, eventsStream, controller);
      } catch (error) {
        debugLog("Error reading stream:", error);
        return null;
      }
    },
  });
}

// appends a fence command with the previous fencing token as null
// (overriding the previous fencing token)
async function stopStream(streamId: string): Promise<void> {
  const { accessToken, basin } = getS2Config();
  const s2 = new S2({ accessToken });
  debugLog("Stopping stream:", streamId);

  try {
    await appendFenceCommand(s2, basin, streamId, null, "end-" + generateFencingToken());
  } catch (error) {
    debugLog("Error stopping stream:", error);
    throw error;
  }
}

async function appendRecords(
  s2: S2,
  basin: string,
  streamId: string,
  appendInput: AppendInput
): Promise<void> {
  try {
    await s2.records.append({
      s2Basin: basin,
      stream: streamId,
      appendInput: {
        records: appendInput.records.records,
        fencingToken: appendInput.fencingToken,
        matchSeqNum: appendInput.matchSeqNum,
      },
    });
  } catch (error: any) {
    if (error instanceof SeqNum) {
      // adding a matchSeqNum enforces that the seqNum assigned to the first record matches,
      // i.e. in the case of retries, it helps de-duplicate records
      debugLog("seqNum mismatch, skipping batch");
      return;
    }
    throw error;
  }
}

async function appendFenceCommand(
  s2: S2,
  basin: string,
  streamId: string,
  prevFencingToken: string | null,
  newFencingToken: string
): Promise<void> {
  await s2.records.append({
    s2Basin: basin,
    stream: streamId,
    appendInput: {
      fencingToken: prevFencingToken,
      records: [
        {
          body: newFencingToken,
          headers: [["", "fence"]],
        },
      ],
    },
  });
}

async function processStream(
  streamID: string,
  eventStream: EventStream<ReadEvent>,
  controller: ReadableStreamDefaultController<string>
): Promise<void> {
  for await (const readEvent of eventStream) {
    if (readEvent.event !== "batch") continue;

    const batch = readEvent.data as ReadBatch;
    for (const rec of batch.records) {
      if (isFenceCommand(rec)) {
        if (rec.body?.startsWith("end")) {
          debugLog("Closing stream due to fence(end) command:", streamID);
          controller.close();
          return;
        }
        continue;
      }
      if (rec.body) {
        try {
          controller.enqueue(rec.body);
        } catch (error: any) {
          if (error.code === "ERR_INVALID_STATE") {
            debugLog("Likely page refresh caused stream closure:", streamID);
            return;
          }
          throw error;
        }
      }
    }
  }
  debugLog("Closing stream due to completion:", streamID);
  controller.close();
}

function isFenceCommand(record: SequencedRecord): boolean {
  return (
    record.headers?.length === 1 && record.headers[0][0] === "" && record.headers[0][1] === "fence"
  );
}

function isStreamDone(readBatch: ReadBatch): boolean {
  if (readBatch.records.length === 0) {
    return false;
  }

  const lastRecord = readBatch.records[0];
  if (!isFenceCommand(lastRecord)) {
    return false;
  }

  const fenceBody = lastRecord.body;
  return (
    fenceBody !== null &&
    fenceBody !== undefined &&
    (fenceBody.startsWith("end-") || fenceBody.startsWith("error-"))
  );
}

function debugLog(...messages: unknown[]) {
  if (process.env.DEBUG || process.env.NODE_ENV === "test") {
    console.log(...messages);
  }
}
