import { S2 } from "@s2-dev/streamstore";
import { ReadAcceptEnum } from '@s2-dev/streamstore/sdk/records.js';
import { EventStream } from '@s2-dev/streamstore/lib/event-streams.js';
import { ReadBatch, ReadEvent, SequencedRecord } from '@s2-dev/streamstore/models/components';

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
   * Maximum time in milliseconds to wait before flushing a batch.
   * Defaults to 1000ms if not set.
   */
  readonly lingerTimeMs: number;
}

interface BatchState {
  records: string[];
  isFirstBatch: boolean;
  lingerTimer?: NodeJS.Timeout;
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
    stream: ReadableStream<string>
  ) => Promise<ReadableStream<string> | null>;
  /**
   * Resumes a previously created stream by its ID.
   * @param streamId - Unique identifier for the stream to resume.
   * @returns A ReadableStream that can be used to read.
  */
  resumeStream: (streamId: string) => ReadableStream<string | null>;
}

function generateFencingToken(): string {
  return Math.random().toString(36).slice(2, 7);
}

function getS2Config(): S2Config {
  const accessToken = process.env.S2_ACCESS_TOKEN;
  const basin = process.env.S2_BASIN;
  const batchSize = parseInt(process.env.S2_BATCH_SIZE ?? "10", 10);
  const lingerTimeMs = parseInt(process.env.S2_LINGER_TIME_MS ?? "100", 10);

  if (!accessToken) throw new Error("S2_ACCESS_TOKEN is not set");
  if (!basin) throw new Error("S2_BASIN is not set");

  return { accessToken, basin, batchSize, lingerTimeMs };
}

export function createResumableStreamContext(
  options: CreateResumableStreamContextOptions
): ResumableStreamContext {
  const ctx = {
    waitUntil: options.waitUntil,
  } as CreateResumableStreamContext;

  getS2Config();
  return {
    resumableStream: async (streamId: string, inputStream: ReadableStream<string>) => {
      return await createResumableStream(ctx, inputStream, streamId);
    },
    resumeStream: (streamId: string) => {
      return resumeStream(streamId);
    },
  };
}

export async function createResumableStream(
  ctx: CreateResumableStreamContext,
  stream: ReadableStream<string>,
  streamId: string
): Promise<ReadableStream<string>> {
  const { accessToken, basin, batchSize, lingerTimeMs } = getS2Config();
  const s2 = new S2({ accessToken });
  const [persistentStream, clientStream] = stream.tee();
  const sessionFencingToken = "session-" + generateFencingToken();

  const processPersistentStream = async () => {
    const reader = persistentStream.getReader();
    const batchState: BatchState = {
      records: [],
      isFirstBatch: true,
    };

    const flushBatch = async () => {
      if (batchState.records.length > 0) {
        await appendRecords(s2, basin, streamId, batchState.records, sessionFencingToken, batchState.isFirstBatch);
        batchState.isFirstBatch = false;
        batchState.records = [];
      }
      if (batchState.lingerTimer) {
        clearTimeout(batchState.lingerTimer);
        batchState.lingerTimer = undefined;
      }
    };

    const startLingerTimer = () => {
      if (batchState.lingerTimer) {
        clearTimeout(batchState.lingerTimer);
      }
      batchState.lingerTimer = setTimeout(async () => {
        try {
          await flushBatch();
        } catch (error) {
          debugLog("Error flushing batch on linger timeout:", error);
        }
      }, lingerTimeMs);
    };

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) {
          if (batchState.lingerTimer) {
            clearTimeout(batchState.lingerTimer);
          }
          if (batchState.records.length > 0) {
            await appendRecords(s2, basin, streamId, batchState.records, sessionFencingToken, batchState.isFirstBatch);
          }
          await appendFenceCommand(s2, basin, streamId, sessionFencingToken, "end-" + generateFencingToken());
          break;
        }

        batchState.records.push(value);
        
        if (batchState.records.length === 1) {
          startLingerTimer();
        }

        if (batchState.records.length >= batchSize) {
          await flushBatch();
        }
      }
    } catch (error) {
      debugLog("Error processing stream:", error);
      if (batchState.lingerTimer) {
        clearTimeout(batchState.lingerTimer);
      }
      try {
        await appendFenceCommand(s2, basin, streamId, sessionFencingToken, "error-" + generateFencingToken());
      } catch (fenceError) {
        debugLog("Error appending fence command:", fenceError);
      }
    } finally {
      reader.releaseLock();
    }
  };

  ctx.waitUntil(processPersistentStream());
  return clientStream;
}

function resumeStream(streamId: string): ReadableStream<string | null> {
  const { accessToken, basin } = getS2Config();
  const s2 = new S2({ accessToken });
  return new ReadableStream({
    async start(controller) {
      try {
        const records = await s2.records.read(
          {
            s2Basin: basin,
            stream: streamId,
            seqNum: 0,
          },
          {
            acceptHeaderOverride: ReadAcceptEnum.textEventStream,
          }
        );
        const recordsStream = records as EventStream<ReadEvent>;
        await processStream(recordsStream, controller);
      } catch (error) {
        debugLog("Error reading stream:", error);
        return null;
      }
    },
  });
}

async function appendRecords(
  s2: S2,
  basin: string,
  streamId: string,
  batch: string[],
  fencingToken: string,
  isFirstBatch?: boolean
): Promise<void> {
  if (isFirstBatch === true) {
    await appendFenceCommand(s2, basin, streamId, "", fencingToken);
  }
  await s2.records.append({
    s2Basin: basin,
    stream: streamId,
    appendInput: {
      records: batch.map((body) => ({ body })),
      fencingToken,
      matchSeqNum: isFirstBatch ? 1 : undefined,
    },
  });
}

async function appendFenceCommand(s2: S2, basin: string, streamId: string, prevFencingToken: string, newFencingToken: string): Promise<void> {
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
  recordsStream: EventStream<ReadEvent>,
  controller: ReadableStreamDefaultController<string>
): Promise<void> {
  for await (const record of recordsStream) {
    if (record.event !== "batch") continue;
    const batch = record.data as ReadBatch;
    for (const rec of batch.records) {
      if (isFenceCommand(rec)) {
        if (rec.body?.startsWith("end")) {
          controller.close();
          return;
        }
        continue;
      }
      if (rec.body) {
        controller.enqueue(rec.body);
      }
    }
  }
  controller.close();
}

function isFenceCommand(record: SequencedRecord): boolean {
  return (
    record.headers?.length === 1 && record.headers[0][0] === "" && record.headers[0][1] === "fence"
  );
}

function debugLog(...messages: unknown[]) {
  if (process.env.DEBUG || process.env.NODE_ENV === "test") {
    console.log(...messages);
  }
}
