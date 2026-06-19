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

package org.apache.hop.parquet.transforms.output;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/** Unit test for {@link ParquetOutputData} */
class ParquetOutputDataTest {

  @Test
  void testDefaultValues() {
    ParquetOutputData data = new ParquetOutputData();
    assertNull(data.sourceFieldIndexes);
    assertNull(data.outputFields);
    assertNull(data.conf);
    assertNull(data.props);
    assertNull(data.filename);
    assertNull(data.outputStream);
    assertNull(data.countingStream);
    assertNull(data.outputFile);
    assertNull(data.writer);
    assertEquals(0, data.split);
    assertEquals(0, data.splitRowCount);
    assertEquals(0, data.maxSplitSizeRows);
    assertEquals(0, data.rowGroupSize);
    assertEquals(0, data.pageSize);
    assertEquals(0, data.dictionaryPageSize);
    assertNull(data.avroSchema);
  }
}
