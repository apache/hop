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

package org.apache.hop.pipeline.transforms.addsnowflakeid;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * SnowflakeIdUtils test
 *
 * @author lance
 * @since 2025/10/16 21:05
 */
class SnowflakeSafeIdGeneratorTests {

  @Test
  void testDefaultNextId() {
    SnowflakeSafeIdGenerator generator = SnowflakeSafeIdGenerator.createDefault();
    long preId = 766649250213851136L;
    long id = generator.nextId();

    Assertions.assertTrue(id > preId);
  }

  @Test
  void testNextId() {
    long dataCenterId = 1;
    long machineId = 10;
    SnowflakeSafeIdGenerator generator = new SnowflakeSafeIdGenerator(dataCenterId, machineId);

    long preId = 766649250213851136L;
    long id = generator.nextId();

    Assertions.assertTrue(id > preId);
  }

  @Test
  void testNextIdWithMaxBackwardsMs() {
    long dataCenterId = 1;
    long machineId = 10;
    long maxBackwardsMs = 30;

    SnowflakeSafeIdGenerator generator =
        new SnowflakeSafeIdGenerator(dataCenterId, machineId, maxBackwardsMs);

    long preId = 766649250213851136L;
    long id = generator.nextId();

    Assertions.assertTrue(id > preId);
  }
}
