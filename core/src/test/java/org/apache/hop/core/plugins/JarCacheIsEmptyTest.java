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

package org.apache.hop.core.plugins;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class JarCacheIsEmptyTest {

  @AfterEach
  void tearDown() {
    JarCache.getInstance().clear();
  }

  @Test
  void clearedCacheIsEmpty() {
    JarCache cache = JarCache.getInstance();
    cache.clear();

    assertTrue(cache.isEmpty());
  }

  @Test
  void cacheWithAnIndexIsNotEmptyUntilCleared() throws Exception {
    JarCache cache = JarCache.getInstance();
    cache.clear();
    // The test class path holds at least one jar or folder with an annotation index
    assumeFalse(cache.getNativeJars().isEmpty(), "no annotation index on the test class path");

    assertFalse(cache.isEmpty());

    cache.clear();

    assertTrue(cache.isEmpty());
  }
}
