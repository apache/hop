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

import java.util.List;
import org.apache.hop.core.exception.HopException;

/** Loads one REST API page at a time; returns an empty list when there is no more data. */
interface GitResourcePageLoader {

  List<GitResourceRecord> loadNextPage() throws HopException;

  /**
   * Whether the underlying source has run out of data.
   *
   * <p>A page can map to no rows while more pages remain: a GitHub issues page that happens to hold
   * only pull requests, for example, since those are filtered out. The reader has to be able to
   * tell that apart from the genuine end of the data, or it stops early and reports a successful
   * run over an incomplete result set.
   */
  boolean isExhausted();

  /** Releases resources held by the loader. Must tolerate repeated calls. */
  default void close() {
    // nothing to release by default
  }

  /** See {@link GitResourceReader#getTruncationNote()}. */
  default String getTruncationNote() {
    return null;
  }
}
