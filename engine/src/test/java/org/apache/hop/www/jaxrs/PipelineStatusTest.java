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

package org.apache.hop.www.jaxrs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import org.apache.hop.pipeline.transform.TransformStatus;
import org.junit.jupiter.api.Test;

class PipelineStatusTest {

  @Test
  void accessorsAndAddTransformStatus() {
    PipelineStatus ps = new PipelineStatus();
    ps.setId("id1");
    ps.setName("pipe");
    ps.setStatus("Running");
    assertEquals("id1", ps.getId());
    assertEquals("pipe", ps.getName());
    assertEquals("Running", ps.getStatus());

    TransformStatus ts = new TransformStatus();
    ps.addTransformStatus(ts);
    assertEquals(1, ps.getTransformStatuses().size());
    assertSame(ts, ps.getTransformStatuses().get(0));

    ps.setTransformStatuses(Collections.emptyList());
    assertTrue(ps.getTransformStatuses().isEmpty());
  }
}
