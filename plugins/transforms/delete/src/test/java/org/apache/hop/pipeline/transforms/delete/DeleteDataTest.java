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

package org.apache.hop.pipeline.transforms.delete;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/** Unit test for {@link DeleteData} */
class DeleteDataTest {

  @Test
  void testDefaultConstructor() {
    DeleteData data = new DeleteData();
    assertNull(data.db);
    assertNull(data.keynrs);
    assertNull(data.keynrs2);
    assertNull(data.outputRowMeta);
    assertNull(data.schemaTable);
    assertNull(data.deleteParameterRowMeta);
    assertNull(data.prepStatementDelete);
  }

  @Test
  void testFieldsCanBeAssigned() {
    DeleteData data = new DeleteData();
    data.schemaTable = "public.customers";
    assertNotNull(data.schemaTable);
    assertEquals("public.customers", data.schemaTable);
  }
}
