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

package org.apache.hop.pipeline.transforms.coalesce;

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.junit.Before;
import org.junit.Test;

public class CoalesceMetaInjectionTest extends BaseMetadataInjectionTest<CoalesceMeta> {
  @Before
  public void setup() throws Exception {
    setup(new CoalesceMeta());
  }

  @Test
  public void test() throws Exception {
    check("EMPTY_STRING_AS_NULLS", () -> meta.isTreatEmptyStringsAsNulls());
    check("NAME", () -> meta.getFields().get(0).getName());
    // check( "TYPE", () -> meta.getOperations().get(0).getType() );
    skipPropertyTest("TYPE");
    check("INPUT_FIELDS", () -> meta.getFields().get(0).getInputFields());
    check("REMOVE_INPUT_FIELDS", () -> meta.getFields().get(0).isRemoveFields());
  }
}
