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


package org.apache.hop.pipeline.transforms.metainject;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class MetaInjectMetaInjectionTest extends BaseMetadataInjectionTest<MetaInjectMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  private static final String TEST_ID = "TEST_ID";

  @Before
  public void setup() throws Exception {
    setup(new MetaInjectMeta());
  }

  @Test
  public void test() throws Exception {
    check("FILE_NAME", () -> meta.getFileName());
    check("SOURCE_TRANSFORM_NAME", () -> meta.getSourceTransformName());
    check("TARGET_FILE", () -> meta.getTargetFile());
    check("NO_EXECUTION", () -> meta.isNoExecution());
    check("STREAMING_SOURCE_TRANSFORM", () -> meta.getStreamSourceTransformName());
    check("STREAMING_TARGET_TRANSFORM", () -> meta.getStreamTargetTransformName());
    check("SOURCE_OUTPUT_NAME", () -> meta.getSourceOutputFields().get(0).getName());
    String[] typeNames = ValueMetaBase.getAllTypes();

    checkStringToInt(
        "SOURCE_OUTPUT_TYPE",
        () -> meta.getSourceOutputFields().get(0).getType(),
        typeNames,
        getTypeCodes(typeNames));
    check("SOURCE_OUTPUT_LENGTH", () -> meta.getSourceOutputFields().get(0).getLength());
    check("SOURCE_OUTPUT_PRECISION", () -> meta.getSourceOutputFields().get(0).getPrecision());
    check(
        "MAPPING_SOURCE_TRANSFORM", () -> meta.getMetaInjectMapping().get(0).getSourceTransform());
    check("MAPPING_SOURCE_FIELD", () -> meta.getMetaInjectMapping().get(0).getSourceField());
    check(
        "MAPPING_TARGET_TRANSFORM", () -> meta.getMetaInjectMapping().get(0).getTargetTransform());
    check("MAPPING_TARGET_FIELD", () -> meta.getMetaInjectMapping().get(0).getTargetField());
  }
}
