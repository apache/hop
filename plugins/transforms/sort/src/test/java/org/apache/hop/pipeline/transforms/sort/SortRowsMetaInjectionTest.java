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

package org.apache.hop.pipeline.transforms.sort;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.injection.BaseMetadataInjectionTestJunit5;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class SortRowsMetaInjectionTest extends BaseMetadataInjectionTestJunit5<SortRowsMeta> {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeEach
  public void setup() throws Exception {
    SortRowsMeta sortRowsMeta = new SortRowsMeta();
    SortRowsField sortRowsField = new SortRowsField();
    List<SortRowsField> sortRowsFields = new ArrayList<>();
    sortRowsFields.add(sortRowsField);
    setup(sortRowsMeta);
  }

  @Test
  public void test() throws Exception {
    check("SORT_DIRECTORY", () -> meta.getDirectory());
    check("SORT_FILE_PREFIX", () -> meta.getPrefix());
    check("SORT_SIZE_ROWS", () -> meta.getSortSize());
    check("FREE_MEMORY_TRESHOLD", () -> meta.getFreeMemoryLimit());
    check("ONLY_PASS_UNIQUE_ROWS", () -> meta.isOnlyPassingUniqueRows());
    check("COMPRESS_TEMP_FILES", () -> meta.isCompressFiles());
    check("NAME", () -> meta.getSortFields().get(0).getFieldName());
    check("SORT_ASCENDING", () -> meta.getSortFields().get(0).isAscending());
    check("IGNORE_CASE", () -> meta.getSortFields().get(0).isCaseSensitive());
    check("PRESORTED", () -> meta.getSortFields().get(0).isPreSortedField());
    check("COLLATOR_ENABLED", () -> meta.getSortFields().get(0).isCollatorEnabled());
    check("COLLATOR_STRENGTH", () -> meta.getSortFields().get(0).getCollatorStrength());
    check("COMPRESS_VARIABLE", () -> meta.getCompressFilesVariable());
    //    check("NAME", () -> meta.getFieldName()[0]);
    //    check("SORT_ASCENDING", () -> meta.getAscending()[0]);
    //    check("IGNORE_CASE", () -> meta.getCaseSensitive()[0]);
    //    check("PRESORTED", () -> meta.getPreSortedField()[0]);
    //    check("COLLATOR_STRENGTH", () -> meta.getCollatorStrength()[0]);
    //    check("COLLATOR_ENABLED", () -> meta.getCollatorEnabled()[0]);
  }
}
