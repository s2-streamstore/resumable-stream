## Resumable stream

This package is inspired by Vercel's take on [Resumable Streams](https://github.com/vercel/resumable-stream) used in the Chat SDK, except instead of Redis, this relies on [S2](http://s2.dev/) providing a basic implementation to create and resume streams.

## Usage

To use this package, you need to create an S2 [access token](https://s2.dev/docs/access-control) and basin to store all your streams.

1. Sign up [here](https://s2.dev/dashboard), generate an access token and set it as `S2_ACCESS_TOKEN` in your env.

2. Create a new basin from the `Basins` tab with the `Create Stream on Append` option enabled, and set it as `S2_BASIN` in your env.

The incoming stream is batched and the batch size can be changed by setting `S2_BATCH_SIZE`.

To integrate this package with the Chat SDK, checkout the following changes [here](https://github.com/s2-streamstore/ai-chatbot/blob/s2-streams/app/(chat)/api/chat/route.ts).

```ts
import { createResumableStreamContext } from "resumable-stream";
import { after } from "next/server";

const streamContext = createResumableStreamContext({
  waitUntil: after,  
});

export async function POST(req: NextRequest, { params }: { params: Promise<{ streamId: string }> }) {
  const { streamId } = await params;
  const inputStream = makeTestStream();
  const stream = await streamContext.createNewResumableStream(
    streamId,
    inputStream,
  );
  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
    },
  });
}

export async function GET(req: NextRequest, { params }: { params: Promise<{ streamId: string }> }) {
  const { streamId } = await params;  
  const stream = await streamContext.resumeExistingStream(
    streamId    
  );
  if (!stream) {
    return new Response("Stream is already done", {
      status: 422,
    });
  }
  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
    },
  });
}
```

## Type docs

[Type docs](./docs/)
