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
 * AddSnowflakeIdMeta test
 *
 * @author lance
 * @since 2025/10/17 10:21
 */
class AddSnowflakeIdMetaTests {

  @Test
  void setDefault() {
    AddSnowflakeIdMeta meta = new AddSnowflakeIdMeta();
    meta.setDefault();

    Assertions.assertEquals(1, meta.getMachineId());
    Assertions.assertEquals(1, meta.getDataCenterId());
    Assertions.assertEquals("snowflakeId", meta.getValueName());
  }

  @Test
  void testClone() {
    AddSnowflakeIdMeta meta = new AddSnowflakeIdMeta();
    meta.setValueName("snowflake_id");
    meta.setDataCenterId(10);
    meta.setMachineId(20);

    AddSnowflakeIdMeta cloned = (AddSnowflakeIdMeta) meta.clone();

    Assertions.assertNotNull(cloned);
    Assertions.assertNotSame(meta, cloned);
    Assertions.assertEquals(meta.getValueName(), cloned.getValueName());
    Assertions.assertEquals(meta.getDataCenterId(), cloned.getDataCenterId());
    Assertions.assertEquals(meta.getMachineId(), cloned.getMachineId());
  }
}
