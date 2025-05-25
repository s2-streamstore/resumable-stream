[**Resumable Stream v1.0.0**](../README.md)

***

[Resumable Stream](../README.md) / ResumableStreamContext

# Interface: ResumableStreamContext

## Properties

### resumableStream()

> **resumableStream**: (`streamId`, `stream`) => `Promise`\<`null` \| `ReadableStream`\<`string`\>\>

Creates a resumable stream from the provided input stream.
The stream will be persisted in S2 and can be resumed later.

#### Parameters

##### streamId

`string`

Unique identifier for the stream.

##### stream

`ReadableStream`\<`string`\>

ReadableStream of data to be persisted.

#### Returns

`Promise`\<`null` \| `ReadableStream`\<`string`\>\>

A ReadableStream that can be used to read.

***

### resumeStream()

> **resumeStream**: (`streamId`) => `ReadableStream`\<`null` \| `string`\>

Resumes a previously created stream by its ID.

#### Parameters

##### streamId

`string`

Unique identifier for the stream to resume.

#### Returns

`ReadableStream`\<`null` \| `string`\>

A ReadableStream that can be used to read.
