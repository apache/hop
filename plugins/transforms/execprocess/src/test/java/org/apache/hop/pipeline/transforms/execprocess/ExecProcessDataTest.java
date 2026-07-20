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

package org.apache.hop.pipeline.transforms.execprocess;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.junit.jupiter.api.Test;

/** Unit test for {@link ExecProcessData} */
class ExecProcessDataTest {

  @Test
  void constructorInitializesDefaults() {
    ExecProcessData data = new ExecProcessData();

    assertEquals(-1, data.indexOfProcess);
    assertNull(data.argumentIndexes);
    assertNull(data.outputRowMeta);
    assertNull(data.runtime);
  }

  @Test
  void implementsITransformData() {
    ExecProcessData data = new ExecProcessData();
    assertTrue(data instanceof ITransformData);
  }

  @Test
  void fieldAssignmentsRoundTrip() {
    ExecProcessData data = new ExecProcessData();

    List<Integer> argumentIndexes = new ArrayList<>();
    argumentIndexes.add(1);
    argumentIndexes.add(3);
    IRowMeta outputRowMeta = new RowMeta();
    Runtime runtime = Runtime.getRuntime();

    data.indexOfProcess = 2;
    data.argumentIndexes = argumentIndexes;
    data.outputRowMeta = outputRowMeta;
    data.runtime = runtime;

    assertEquals(2, data.indexOfProcess);
    assertSame(argumentIndexes, data.argumentIndexes);
    assertEquals(2, data.argumentIndexes.size());
    assertEquals(1, data.argumentIndexes.get(0));
    assertEquals(3, data.argumentIndexes.get(1));
    assertSame(outputRowMeta, data.outputRowMeta);
    assertNotNull(data.runtime);
    assertSame(runtime, data.runtime);
  }
}
