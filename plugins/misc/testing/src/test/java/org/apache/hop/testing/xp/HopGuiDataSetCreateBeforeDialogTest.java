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

package org.apache.hop.testing.xp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Test;

class HopGuiDataSetCreateBeforeDialogTest {

  @Test
  void singleSelectedTransformNameUsesTheOnlySelection() {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.addTransform(transform("Ignored", false));
    pipelineMeta.addTransform(transform("Check", true));

    assertEquals(
        "Check", HopGuiDataSetCreateBeforeDialog.singleSelectedTransformName(pipelineMeta));
  }

  @Test
  void singleSelectedTransformNameIgnoresZeroOrSeveralSelections() {
    assertNull(HopGuiDataSetCreateBeforeDialog.singleSelectedTransformName(null));

    PipelineMeta none = new PipelineMeta();
    none.addTransform(transform("Check", false));
    assertNull(HopGuiDataSetCreateBeforeDialog.singleSelectedTransformName(none));

    PipelineMeta several = new PipelineMeta();
    several.addTransform(transform("One", true));
    several.addTransform(transform("Two", true));
    assertNull(HopGuiDataSetCreateBeforeDialog.singleSelectedTransformName(several));
  }

  private static TransformMeta transform(String name, boolean selected) {
    TransformMeta transformMeta = new TransformMeta(name, null);
    if (selected) {
      transformMeta.flipSelected();
    }
    return transformMeta;
  }
}
