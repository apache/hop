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

package org.apache.hop.spark.transforms.table;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.injection.BaseMetadataInjectionTestJunit5;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.spark.transforms.io.SparkField;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class SparkLakeTableInputMetaInjectionTest
    extends BaseMetadataInjectionTestJunit5<SparkLakeTableInputMeta> {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  @BeforeEach
  void setup() throws Exception {
    SparkLakeTableInputMeta meta = new SparkLakeTableInputMeta();
    List<SparkField> fields = new ArrayList<>();
    fields.add(new SparkField());
    meta.setFields(fields);
    setup(meta);
  }

  @Test
  void test() throws Exception {
    check("FORMAT", () -> meta.getFormat());
    check("IDENTIFIER_MODE", () -> meta.getIdentifierMode());
    check("TABLE_PATH", () -> meta.getTablePath());
    check("TABLE_IDENTIFIER", () -> meta.getTableIdentifier());
    check("CATALOG_METADATA_NAME", () -> meta.getCatalogMetadataName());
    check("TIME_TRAVEL_TYPE", () -> meta.getTimeTravelType());
    check("TIME_TRAVEL_VERSION", () -> meta.getTimeTravelVersion());
    check("TIME_TRAVEL_TIMESTAMP", () -> meta.getTimeTravelTimestamp());
    check("EXTRA_OPTIONS", () -> meta.getExtraOptions());
    check("NAME", () -> meta.getFields().get(0).getName());
    check("TYPE", () -> meta.getFields().get(0).getHopType());
    check("LENGTH", () -> meta.getFields().get(0).getLength());
    check("PRECISION", () -> meta.getFields().get(0).getPrecision());
    check("FORMAT_MASK", () -> meta.getFields().get(0).getFormatMask());
  }
}
