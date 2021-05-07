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

package org.apache.hop.pipeline.transforms.analyticquery;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class AnalyticQueryMetaInjectionTest extends BaseMetadataInjectionTest<AnalyticQueryMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setup() throws Exception {
    setup(new AnalyticQueryMeta());
  }

  @Test
  public void test() throws Exception {
    check("GROUP_FIELDS", () -> meta.getGroupFields().get(0).getFieldName());
    check("OUTPUT.AGGREGATE_FIELD", () -> meta.getQueryFields().get(0).getAggregateField());
    check("OUTPUT.SUBJECT_FIELD", () -> meta.getQueryFields().get(0).getSubjectField());
    check(
        "OUTPUT.AGGREGATE_TYPE",
        () -> meta.getQueryFields().get(0).getAggregateType(),
        QueryField.AggregateType.class);
    check("OUTPUT.VALUE_FIELD", () -> meta.getQueryFields().get(0).getValueField());
  }
}
