/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core.plugins;

import java.util.Objects;

/**
 * The verdict of an engine on a single transform or action plugin: can the engine run it, will it
 * refuse to run it, or does the engine not have an explicit opinion. UNKNOWN is the
 * backwards-compatible default; it means "behave as today" and is treated as supported by the UI
 * (with a subtle indicator) so that the 200-odd transforms that fall through to a generic Beam
 * ParDo wrapper keep working.
 */
public final class EngineCompatibility {

  public enum Verdict {
    SUPPORTED,
    UNSUPPORTED,
    UNKNOWN
  }

  private static final EngineCompatibility SUPPORTED =
      new EngineCompatibility(Verdict.SUPPORTED, null);
  private static final EngineCompatibility UNKNOWN = new EngineCompatibility(Verdict.UNKNOWN, null);

  private final Verdict verdict;
  private final String reason;

  private EngineCompatibility(Verdict verdict, String reason) {
    this.verdict = verdict;
    this.reason = reason;
  }

  public static EngineCompatibility supported() {
    return SUPPORTED;
  }

  public static EngineCompatibility unknown() {
    return UNKNOWN;
  }

  public static EngineCompatibility unsupported(String reason) {
    return new EngineCompatibility(Verdict.UNSUPPORTED, reason);
  }

  public Verdict getVerdict() {
    return verdict;
  }

  /** User-facing message explaining an UNSUPPORTED verdict. Null for SUPPORTED / UNKNOWN. */
  public String getReason() {
    return reason;
  }

  public boolean isSupported() {
    return verdict == Verdict.SUPPORTED;
  }

  public boolean isUnsupported() {
    return verdict == Verdict.UNSUPPORTED;
  }

  public boolean isUnknown() {
    return verdict == Verdict.UNKNOWN;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof EngineCompatibility that)) return false;
    return verdict == that.verdict && Objects.equals(reason, that.reason);
  }

  @Override
  public int hashCode() {
    return Objects.hash(verdict, reason);
  }

  @Override
  public String toString() {
    return reason == null ? verdict.name() : verdict.name() + "(" + reason + ")";
  }
}
