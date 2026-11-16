import type { ICacheService } from "dweb-api-types/cache";
import type { ILoggerService } from "dweb-api-types/logger";
import type { IRequestContext } from "dweb-api-types/request-context";
import type { ZodType } from "zod";
import { InflightLookups } from "./inflight.js";

/**
 * Wraps a cache so that concurrent memoize calls for the same key share one
 * call to the wrapped cache.
 *
 * Without it every request that misses the cache starts its own lookup, so
 * a name that isn't cached yet (or whose entry just expired) costs one rpc
 * round trip per concurrent request instead of one in total. The wrapped
 * cache doesn't know about any of this.
 */
export class SharedLookupCacheService implements ICacheService {
  private readonly lookups: InflightLookups;

  /**
   * @param maxJoinMs how long a running lookup can be joined. A healthy one
   *   is one or two rpc round trips, so the default leaves plenty of room
   *   while not letting new requests pile onto one that hangs.
   */
  public constructor(
    private readonly logger: ILoggerService,
    private readonly inner: ICacheService,
    maxJoinMs = 5000,
  ) {
    // Memoized values pass a zod schema and are stored in redis as JSON, so
    // they're plain data and structuredClone can copy them.
    this.lookups = new InflightLookups(maxJoinMs, structuredClone);
  }

  public memoize<RT>(
    request: IRequestContext,
    fThunk: () => Promise<Awaited<RT>>,
    schema: ZodType<RT>,
    dbPrefix: string,
    key: string,
  ): Promise<Awaited<RT>> {
    const cKey = `${dbPrefix}/${key}`;
    if (this.lookups.joinable(cKey)) {
      this.logger.debug("waiting for a lookup that is already running", {
        ...request,
        origin: "SharedLookupCacheService",
        context: { key: cKey },
      });
    }
    return this.lookups.run(cKey, () =>
      this.inner.memoize(request, fThunk, schema, dbPrefix, key),
    );
  }

  public getTtl(
    request: IRequestContext,
    dbPrefix: string,
    key: string,
  ): Promise<number | undefined> {
    return this.inner.getTtl(request, dbPrefix, key);
  }
}
