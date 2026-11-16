type RunningLookup = {
  promise: Promise<unknown>;
  startedAt: number;
};

/**
 * Lets callers that ask for the same key at the same time share one lookup.
 *
 * The first caller runs the lookup and gets its result as is. Callers that
 * join get the result passed through copyForJoiner, so they don't all hold
 * the same object. What a copy means depends on the values, so the owner
 * decides.
 *
 * If the lookup fails, the caller that started it gets the error. The ones
 * that joined try once more, together: one more lookup in total, not one
 * each. On their own each of them would have made its own attempt, so one
 * failed attempt (a dropped connection, one bad response) shouldn't fail
 * everyone who happened to join it. If the second attempt fails too, they
 * all get that error. There's no second attempt after a lookup that failed
 * slowly (see below).
 *
 * A lookup older than maxJoinMs isn't joined anymore, the next caller starts
 * a new one. A normal lookup is done long before that, so one that is still
 * running is probably stuck, and callers arriving later shouldn't all wait
 * on it.
 */
export class InflightLookups {
  private readonly running = new Map<string, RunningLookup>();

  constructor(
    private readonly maxJoinMs: number,
    private readonly copyForJoiner: <T>(value: T) => T,
    private readonly now: () => number = Date.now,
  ) {}

  // Whether run(key) would join a lookup that is already running.
  joinable(key: string): boolean {
    return this.joinableLookup(key) !== undefined;
  }

  run<T>(key: string, lookup: () => Promise<T>): Promise<T> {
    return this.runOrJoin(key, lookup, true);
  }

  get size(): number {
    return this.running.size;
  }

  private runOrJoin<T>(
    key: string,
    lookup: () => Promise<T>,
    retryIfItFails: boolean,
  ): Promise<T> {
    const running = this.joinableLookup(key);
    if (!running) {
      return this.start(key, lookup);
    }

    return running.promise.then(
      (value) => this.copyForJoiner(value as T),
      (error) => {
        // A failure that came back quickly is worth one more try. One that
        // took maxJoinMs or longer was most likely a timeout, and another
        // attempt would only double how long these callers take to fail.
        if (
          !retryIfItFails ||
          this.now() - running.startedAt >= this.maxJoinMs
        ) {
          throw error;
        }
        // The first one to get here starts the second attempt, the others
        // find it running and join it, this time without another retry.
        // start() has usually dropped the failed one already, its callback
        // was registered first, but this shouldn't depend on that order.
        this.forget(key, running);
        return this.runOrJoin(key, lookup, false);
      },
    );
  }

  private start<T>(key: string, lookup: () => Promise<T>): Promise<T> {
    // async wrapper so a lookup that throws synchronously still ends up as
    // a rejected promise and gets removed below
    const promise = (async () => lookup())();
    const entry: RunningLookup = { promise, startedAt: this.now() };
    this.running.set(key, entry);

    const forget = () => this.forget(key, entry);
    promise.then(forget, forget);

    return promise;
  }

  private joinableLookup(key: string): RunningLookup | undefined {
    const running = this.running.get(key);
    if (running && this.now() - running.startedAt < this.maxJoinMs) {
      return running;
    }
    return undefined;
  }

  // A newer lookup may have replaced this one in the meantime (after
  // maxJoinMs), don't remove that one.
  private forget(key: string, entry: RunningLookup) {
    if (this.running.get(key) === entry) {
      this.running.delete(key);
    }
  }
}
