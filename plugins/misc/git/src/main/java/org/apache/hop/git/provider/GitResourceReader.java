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

package org.apache.hop.git.provider;

import org.apache.hop.core.exception.HopException;

/** Streams Git resources page-by-page instead of loading everything upfront. */
public interface GitResourceReader extends AutoCloseable {

  boolean hasNext() throws HopException;

  GitResourceRecord next() throws HopException;

  /** Number of records returned so far. */
  int count();

  /**
   * Releases any provider resources held by this reader. Implementations must tolerate being closed
   * more than once, and being closed before the underlying data is exhausted.
   */
  @Override
  default void close() {
    // nothing to release by default
  }

  /**
   * A note describing why iteration stopped before the underlying data was exhausted, or {@code
   * null} when all requested rows were read. Used for logging; a provider-imposed pagination cap is
   * not an error, but the user needs to know the result set is capped.
   */
  default String getTruncationNote() {
    return null;
  }
}
