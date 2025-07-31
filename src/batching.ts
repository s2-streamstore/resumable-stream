export interface AppendOpts {
  maxBatchRecords?: number;
  matchSeqNum?: number;
  fencingToken?: string;
}

export interface AppendRecord {
  body: string;
  headers?: [string, string][];
}

export interface AppendRecordBatch {
  records: AppendRecord[];
  max_capacity: number;
}

export interface AppendInput {
  records: AppendRecordBatch;
  matchSeqNum?: number;
  fencingToken?: string;
}

const MAX_BATCH_CAPACITY = 1000;
const MIN_BATCH_CAPACITY = 1;

const defaultAppendOpts: AppendOpts = {
  maxBatchRecords: MAX_BATCH_CAPACITY,
  matchSeqNum: undefined,
  fencingToken: undefined,
};

export function createAppendOpts(overrides: AppendOpts = {}): AppendOpts {
  const opts = { ...defaultAppendOpts, ...overrides };

  if (
    opts.maxBatchRecords !== undefined &&
    (opts.maxBatchRecords < MIN_BATCH_CAPACITY || opts.maxBatchRecords > MAX_BATCH_CAPACITY)
  ) {
    throw new Error(
      `Batch capacity must be between ${MIN_BATCH_CAPACITY} and ${MAX_BATCH_CAPACITY}`
    );
  }

  return opts;
}

export class BatchBuilder {
  private peekedRecord: AppendRecord | null = null;
  private nextMatchSeqNum: number | undefined;
  private batch: AppendRecordBatch;

  constructor(private opts: AppendOpts = {}) {
    this.opts = createAppendOpts(opts);
    this.batch = {
      records: [],
      max_capacity: this.opts.maxBatchRecords || MAX_BATCH_CAPACITY,
    };
  }

  addRecord(body: string, headers?: [string, string][]): boolean {
    const record: AppendRecord = { body, headers };

    if (this.batch.records.length >= this.batch.max_capacity) {
      this.peekedRecord = record;
      return false;
    }

    this.batch.records.push(record);
    return true;
  }

  isFull(): boolean {
    return this.batch.records.length >= this.batch.max_capacity;
  }

  hasRecords(): boolean {
    return this.batch.records.length > 0;
  }

  isEmpty(): boolean {
    return this.batch.records.length === 0;
  }

  get length(): number {
    return this.batch.records.length;
  }

  setMatchSeqNum(seqNum: number): void {
    this.nextMatchSeqNum = seqNum;
  }

  flush(): AppendInput | null {
    if (this.isEmpty()) {
      return null;
    }

    const matchSeqNum = this.nextMatchSeqNum;

    if (this.nextMatchSeqNum !== undefined) {
      this.nextMatchSeqNum += this.batch.records.length;
    }

    const flushedBatch = this.batch;
    this.batch = {
      records: [],
      max_capacity: this.opts.maxBatchRecords || MAX_BATCH_CAPACITY,
    };

    const result: AppendInput = {
      records: flushedBatch,
      matchSeqNum,
      fencingToken: this.opts.fencingToken,
    };

    if (this.peekedRecord) {
      this.batch.records.push(this.peekedRecord);
      this.peekedRecord = null;
    }

    console.assert(this.peekedRecord === null, "Record did not fit after flush");

    return result;
  }
}
