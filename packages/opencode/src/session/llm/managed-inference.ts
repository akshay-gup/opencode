/** This identifies a patched source contract, not a stock version whitelist. */
export const MANAGED_INFERENCE = {
  protocol: "metalanguage-inference-v2",
  base: "826d9ad46a22bef0294998e08daa3c4904fea28f",
  acknowledgement: "response-metadata-before-dispatch",
} as const

/** Interpose below the AI SDK's tool executor, not its delayed UI step events.
 * Metadata is emitted from a successful provider response. Preparation, an HTTP
 * failure, stream-start alone, and cancellation before metadata do not count.
 * The caller retains the logical inference ID across all transport retries.
 */
export function acknowledgedStream<T extends { type: string }>(
  stream: ReadableStream<T>,
  acknowledge: (responseID: string) => Promise<void>,
): ReadableStream<T> {
  let responseID: string | undefined
  return stream.pipeThrough(new TransformStream<T, T>({
    async transform(part, controller) {
      if (part.type === "response-metadata") {
        const id = "id" in part ? part.id : undefined
        if (typeof id !== "string" || !id) throw new Error("Managed inference requires a provider response identity")
        if (responseID && responseID !== id) throw new Error("Multiple provider responses share one managed inference")
        if (!responseID) {
          await acknowledge(id)
          responseID = id
        }
      } else if (!responseID && !["stream-start", "error", "raw"].includes(part.type)) {
        throw new Error("Provider emitted model output before managed inference acknowledgement")
      }
      controller.enqueue(part)
    },
    flush() {
      if (!responseID) throw new Error("Provider stream ended without managed inference acknowledgement")
    },
  }))
}
