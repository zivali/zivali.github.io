/**
 * Notion enforces a public API request rate limit (by default 3 requests per
 * second, shared by the whole integration). When the limit is hit, Notion
 * responds with HTTP 429 and a `retry-after` header telling us how long to
 * wait. The official @notionhq/client does NOT retry automatically, so we wrap
 * each request and:
 *  1. delay 350ms between requests to stay under the 3 rps default limit, and
 *  2. on a 429, wait `retry-after` (+ a small buffer) before retrying, up to
 *     `maxRetries` attempts.
 */
const REQUEST_INTERVAL_MS = 350;
const RETRY_BUFFER_MS = 1000;
const MAX_RETRIES = 5;

export type FetchLike = (url: string, init?: object) => Promise<Response>;
// Keep a reference to the global fetch at module load time, so the wrapper
// keeps working even if the global is later swapped (undici in newer Node).
const globalFetch = globalThis.fetch;

/** Promise-chained delay. */
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

let lastRequestAt = 0;

/**
 * Returns a fetch implementation that serializes (and rate-limits) all Notion
 * API requests on the Node.js side. This guarantees we never exceed Notion's
 * default 3 requests/second and retries 429 responses after the requested
 * backoff.
 */
export function makeRateLimitedFetch(): FetchLike {
  return async function rateLimitedFetch(
    url: string,
    init?: object
  ): Promise<Response> {
    for (let attempt = 1; ; attempt++) {
      // Space requests out so bursts never trip the 3 rps public API limit.
      const now = Date.now();
      const wait = Math.max(0, lastRequestAt + REQUEST_INTERVAL_MS - now);
      if (wait > 0) await sleep(wait);
      lastRequestAt = Date.now();

      try {
        // eslint-disable-next-line no-await-in-loop
        const response = await globalFetch(url, init as RequestInit);
        if (response.status === 429) {
          const retryAfterSeconds = Number(
            response.headers.get("retry-after") ?? "1"
          );
          if (attempt >= MAX_RETRIES) return response;
          console.info(
            `[Info] Notion rate limited (429), retrying in ${
              retryAfterSeconds + 1
            }s (attempt ${attempt}/${MAX_RETRIES})`
          );
          await sleep((retryAfterSeconds + 1) * 1000 + RETRY_BUFFER_MS);
          continue;
        }
        return response;
      } catch (err) {
        // Retry transient network failures too.
        if (attempt >= MAX_RETRIES) throw err;
        await sleep(RETRY_BUFFER_MS * attempt);
      }
    }
  };
}
