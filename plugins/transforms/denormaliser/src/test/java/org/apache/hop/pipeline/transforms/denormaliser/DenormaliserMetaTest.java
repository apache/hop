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
 *
 */

package org.apache.hop.pipeline.transforms.denormaliser;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.Set;
import org.apache.hop.metadata.inject.HopMetadataInjector;
import org.junit.jupiter.api.Test;

class DenormaliserMetaTest {
  @Test
  void testMetadataGroupMapping() throws Exception {
    Map<String, Set<String>> map =
        HopMetadataInjector.findInjectionGroupKeys(DenormaliserMeta.class);
    assertEquals(2, map.size());
    Set<String> groupKeys = map.get("group");
    assertEquals(1, groupKeys.size());
    Set<String> fieldKeys = map.get("fields");
    assertEquals(12, fieldKeys.size());
  }
}
