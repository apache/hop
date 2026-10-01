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

package org.apache.hop.pipeline.transforms.constant;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Reproduces issue #8022: HopGuiPipelineTransformDelegate.editTransform() decides whether a
 * transform was edited by comparing TransformMeta.getXml() of a clone taken before the dialog
 * opened against a clone taken after OK. ConstantMeta.clone() is a shallow Object.clone(), so the
 * "before" snapshot shares the very same List instance as the live meta. ConstantDialog.ok()
 * mutates that list in place, so both snapshots serialize identically and the edit is lost.
 */
class ConstantChangeDetectionTest {

  @BeforeAll
  static void setUpBeforeClass() throws Exception {
    HopEnvironment.init();
  }

  private static TransformMeta constantTransform() {
    ConstantMeta meta = new ConstantMeta();
    meta.getFields().add(new ConstantField("a", "String", "one"));
    TransformMeta transformMeta = new TransformMeta("Add constants", meta);
    transformMeta.setTransformPluginId("Constant");
    return transformMeta;
  }

  /** Exactly what the delegate does to decide "did the user change anything?". */
  private static boolean delegateSeesChange(TransformMeta before, TransformMeta after)
      throws Exception {
    return !before.getXml().equals(after.getXml());
  }

  @Test
  void editingConstantFieldsIsDetected() throws Exception {
    TransformMeta transformMeta = constantTransform();
    TransformMeta before = (TransformMeta) transformMeta.clone();

    // What ConstantDialog.ok() does: take the live list, clear it, refill it from the grid.
    List<ConstantField> fields = ((ConstantMeta) transformMeta.getTransform()).getFields();
    fields.clear();
    fields.add(new ConstantField("a", "String", "CHANGED BY THE USER"));

    TransformMeta after = (TransformMeta) transformMeta.clone();

    System.out.println("---- BEFORE ----\n" + before.getXml());
    System.out.println("---- AFTER ----\n" + after.getXml());
    System.out.println(
        "shared list instance: "
            + (((ConstantMeta) before.getTransform()).getFields()
                == ((ConstantMeta) after.getTransform()).getFields()));

    assertEquals(true, delegateSeesChange(before, after), "the delegate must see the edit");
  }
}
