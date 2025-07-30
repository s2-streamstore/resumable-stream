export interface AppendOpts {
  maxBatchRecords?: number;
  matchSeqNum?: number
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

const defaultAppendOpts: AppendOpts = {
  maxBatchRecords: 1000,
  matchSeqNum: undefined,
  fencingToken: undefined,  
};

export function createAppendOpts(overrides: AppendOpts = {}): AppendOpts {
  const opts = { ...defaultAppendOpts, ...overrides };

  if (opts.maxBatchRecords !== undefined && (opts.maxBatchRecords <= 0 || opts.maxBatchRecords > 1000)) {
    throw new Error("Batch capacity must be between 1 and 1000");
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
      max_capacity: this.opts.maxBatchRecords || 1000,
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

  len(): number {
    return this.batch.records.length;
  }

  setMatchSeqNum(seqNum: number): void {
    this.nextMatchSeqNum = seqNum;
  }

  flush(): AppendInput | null {
    if (this.batch.records.length === 0) return null;

    const matchSeqNum = this.nextMatchSeqNum;

    if (this.nextMatchSeqNum !== undefined) {
      this.nextMatchSeqNum += this.batch.records.length;
    }

    const flushedBatch = this.batch;
    this.batch = {
      records: [],
      max_capacity: this.opts.maxBatchRecords || 1000,
    };

    const result: AppendInput = {
      records: flushedBatch,
      matchSeqNum,
      fencingToken: this.opts.fencingToken,
    };

    const leftover = this.peekedRecord;
    this.peekedRecord = null;

    if (leftover) {
      this.batch.records.push(leftover);
    }

    console.assert(this.peekedRecord === null, 'Record did not fit after flush');

    return result;
  }
}
