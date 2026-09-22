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
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;

/** Unit test for {@link ParquetTypeFilename} */
class ParquetTypeFilenameTest {

  @Test
  void filtersParquetFilesFirst() {
    ParquetTypeFilename type = new ParquetTypeFilename();
    assertEquals(".parquet", type.getDefaultFileExtension());
    assertEquals("*.parquet", type.getFilterExtensions()[0]);
    assertEquals(type.getFilterExtensions().length, type.getFilterNames().length);
    for (String name : type.getFilterNames()) {
      assertFalse(name.startsWith("!"), "untranslated filter name: " + name);
    }
  }
}
