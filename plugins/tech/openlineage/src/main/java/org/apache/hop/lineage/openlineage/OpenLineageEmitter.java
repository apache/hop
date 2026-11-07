/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.lineage.openlineage;

import io.openlineage.client.OpenLineage.RunEvent;
import java.util.ArrayList;
import java.util.List;

/**
 * Delivers a single OpenLineage {@link RunEvent}. {@link OpenLineageHttpClient} posts it to an
 * OpenLineage collector; a sink that writes to a collector with its own API (DataHub) translates
 * the event instead. The {@link OpenLineageAsyncDispatcher} calls it from its worker thread.
 */
public interface OpenLineageEmitter extends AutoCloseable {

  /** What became of an event. */
  enum Result {
    /** The collector accepted it. */
    SENT,
    /**
     * It could not be delivered now — the collector is unreachable or overloaded, or the
     * credentials or URL are wrong — and is worth sending again later.
     */
    RETRY,
    /** The collector understood the event and refused it: sending it again cannot succeed. */
    REJECTED
  }

  /** Whether an HTTP status is worth retrying straight away (5xx, 408, 429, or no response). */
  static boolean isTransient(int status) {
    return status < 400 || status >= 500 || status == 408 || status == 429;
  }

  /**
   * What a failed request means for the event once immediate retries are spent. 401, 403, 404 and
   * 407 point at configuration that someone will fix (a token, the URL, a proxy), so the event is
   * kept rather than discarded with every other event sent until then; any other 4xx is a refusal
   * of the event itself.
   */
  static Result failureOf(int status) {
    if (isTransient(status)) {
      return Result.RETRY;
    }
    return switch (status) {
      case 401, 403, 404, 407 -> Result.RETRY;
      default -> Result.REJECTED;
    };
  }

  /** Delivers one event. Must not throw on a delivery failure. */
  Result emit(RunEvent event);

  /**
   * Delivers several events, oldest first, and says what became of each. An emitter that can write
   * them in one request overrides this; by default they are sent one by one.
   */
  default List<Result> emitAll(List<RunEvent> events) {
    List<Result> results = new ArrayList<>(events.size());
    for (RunEvent event : events) {
      results.add(emit(event));
    }
    return results;
  }

  @Override
  void close();
}
