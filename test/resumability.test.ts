import { createResumableStreamContext } from '../src/index.ts';

function createStreamFromArray(data: string[]): ReadableStream<string> {
    let index = 0;
    return new ReadableStream({
        pull(controller) {
            if (index < data.length) {
                controller.enqueue(data[index++]);
            } else {
                controller.close();
            }
        }
    });
}


async function readStreamToArray(
    stream: ReadableStream<string> | ReadableStream<string | null>,
    timeoutMs: number = 5000
): Promise<string[]> {
    const reader = stream.getReader();
    const result: string[] = [];

    const timeoutPromise = new Promise<never>((_, reject) => {
        setTimeout(() => reject(new Error('Stream read timeout')), timeoutMs);
    });

    try {
        while (true) {
            const readPromise = reader.read();
            const { done, value } = await Promise.race([readPromise, timeoutPromise]);

            if (done) break;
            if (value !== null && value !== undefined) {
                result.push(value);
            }
        }
    } finally {
        reader.releaseLock();
    }

    return result;
}

test('pub/sub', async () => {

    const context = createResumableStreamContext({
        waitUntil: async (promise) => {
            await promise;
        }
    });

    const originalData = ['msg1', 'msg2', 'msg3', 'msg4', 'msg5'];
    const streamId = `test-${Date.now()}-${Math.random().toString(36).slice(2)}`;

    const inputStream = createStreamFromArray(originalData);
    const publisherStream = await context.resumableStream(streamId, inputStream);

    const publisherData = await readStreamToArray(publisherStream!);

    await new Promise(resolve => setTimeout(resolve, 1000));

    const resumedStream = context.resumeStream(streamId);
    const subscriberData = await readStreamToArray(resumedStream);

    expect(publisherData).toEqual(originalData);
    expect(subscriberData).toEqual(originalData);
});
