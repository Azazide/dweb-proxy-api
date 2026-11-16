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
    this.lookups = new InflightLookups(maxJoinMs);
  }

  public memoize<RT>(
    request: IRequestContext,
    fThunk: () => Promise<Awaited<RT>>,
    schema: ZodType<RT>,
    dbPrefix: string,
    key: string,
  ): Promise<Awaited<RT>> {
    // Only identifies the lookup here, it isn't the wrapped cache's key.
    const lookupKey = JSON.stringify([dbPrefix, key]);
    if (this.lookups.joinable(lookupKey)) {
      this.logger.debug("waiting for a lookup that is already running", {
        ...request,
        origin: "SharedLookupCacheService",
        context: { dbPrefix, key },
      });
    }
    return this.lookups.run(
      lookupKey,
      () => this.inner.memoize(request, fThunk, schema, dbPrefix, key),
      (value) => copyThroughSchema(schema, value),
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

// Parsing builds new objects and arrays for everything the schema describes,
// so a caller that joined gets its own copy without us assuming anything
// about how the wrapped cache stores values. A value the schema doesn't
// accept is passed on as is, the same as the caller that started the lookup
// gets it.
function copyThroughSchema<RT>(schema: ZodType<RT>, value: Awaited<RT>) {
  const parsed = schema.safeParse(value);
  return parsed.success ? (parsed.data as Awaited<RT>) : value;
}
