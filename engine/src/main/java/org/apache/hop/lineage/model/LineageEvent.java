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

package org.apache.hop.lineage.model;

import java.util.Objects;
import java.util.UUID;
import lombok.Getter;
import org.apache.hop.lineage.context.LineageContext;

/**
 * Immutable lineage observation. The {@code payload} may be null when the {@link LineageEventKind}
 * alone is sufficient.
 */
@Getter
public final class LineageEvent {

  private final String eventId;
  private final long timestampMillis;
  private final LineageEventKind kind;
  private final LineageContext context;
  private final LineagePayload payload;

  public LineageEvent(
      String eventId,
      long timestampMillis,
      LineageEventKind kind,
      LineageContext context,
      LineagePayload payload) {
    this.eventId = Objects.requireNonNull(eventId, "eventId");
    this.timestampMillis = timestampMillis;
    this.kind = Objects.requireNonNull(kind, "kind");
    this.context = Objects.requireNonNull(context, "context");
    this.payload = payload;
  }

  /**
   * Builds an event with a random UUID, {@link System#currentTimeMillis()} as timestamp, and the
   * given kind, context, and payload.
   */
  public static LineageEvent of(
      LineageEventKind kind, LineageContext context, LineagePayload payload) {
    return new LineageEvent(
        UUID.randomUUID().toString(), System.currentTimeMillis(), kind, context, payload);
  }
}
