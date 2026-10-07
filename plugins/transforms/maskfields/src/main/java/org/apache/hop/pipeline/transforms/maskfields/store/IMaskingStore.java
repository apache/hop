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

package org.apache.hop.pipeline.transforms.maskfields.store;

import org.apache.hop.core.exception.HopException;

/** Remembers the replacement string for a source value, scoped by masking pattern name. */
public interface IMaskingStore extends AutoCloseable {

  /**
   * Returns the stored replacement, or allocates one and stores it.
   *
   * <p>The allocator runs only for a new key, while the store holds its lock or transaction. It may
   * call {@link #allocateSequence(String, long)} and {@link #count(String)}.
   */
  String findOrCreate(String patternName, String sourceKey, MaskAllocator allocator)
      throws HopException;

  /** Next sequence value for the pattern, starting at {@code start} the first time. */
  long allocateSequence(String patternName, long start) throws HopException;

  /** Number of source keys already stored for the pattern. */
  int count(String patternName) throws HopException;

  @Override
  void close();

  /** Builds the replacement string for a new source key. */
  @FunctionalInterface
  interface MaskAllocator {
    String allocate(IMaskingStore store) throws HopException;
  }
}
