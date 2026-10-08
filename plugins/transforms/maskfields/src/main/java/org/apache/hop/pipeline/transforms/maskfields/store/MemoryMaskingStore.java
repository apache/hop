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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hop.core.exception.HopException;

/** In-memory mapping. It lives for one pipeline execution and starts again on the next run. */
public class MemoryMaskingStore implements IMaskingStore {

  private final Map<String, Map<String, String>> values = new HashMap<>();
  private final Map<String, AtomicLong> sequences = new HashMap<>();

  @Override
  public synchronized String findOrCreate(
      String patternName, String sourceKey, MaskAllocator allocator) throws HopException {
    Map<String, String> patternValues =
        values.computeIfAbsent(patternName, name -> new HashMap<>());
    String existing = patternValues.get(sourceKey);
    if (existing != null) {
      return existing;
    }
    String created = allocator.allocate(this);
    patternValues.put(sourceKey, created);
    return created;
  }

  @Override
  public synchronized long allocateSequence(String patternName, long start) {
    AtomicLong counter = sequences.computeIfAbsent(patternName, name -> new AtomicLong(start));
    return counter.getAndIncrement();
  }

  @Override
  public synchronized int count(String patternName) {
    Map<String, String> patternValues = values.get(patternName);
    return patternValues == null ? 0 : patternValues.size();
  }

  @Override
  public void close() {
    values.clear();
    sequences.clear();
  }
}
