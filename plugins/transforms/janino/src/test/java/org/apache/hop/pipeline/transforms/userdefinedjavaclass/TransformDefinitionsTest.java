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
package org.apache.hop.pipeline.transforms.userdefinedjavaclass;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.Test;

/**
 * Tests for the four lightweight definition/parameter value objects: {@link TransformDefinition},
 * {@link InfoTransformDefinition}, {@link TargetTransformDefinition}, and {@link UsageParameter}.
 */
class TransformDefinitionsTest {

  // ================================================================== TransformDefinition

  @Test
  void transformDefinition_defaultConstructor_emptyStringsNullMeta() {
    TransformDefinition td = new TransformDefinition();
    assertEquals("", td.tag);
    assertEquals("", td.transformName);
    assertNull(td.transformMeta);
    assertEquals("", td.description);
  }

  @Test
  void transformDefinition_fullConstructor_setsAllFields() {
    TransformMeta tm = mock(TransformMeta.class);
    TransformDefinition td = new TransformDefinition("t", "name", tm, "desc");
    assertEquals("t", td.tag);
    assertEquals("name", td.transformName);
    assertEquals(tm, td.transformMeta);
    assertEquals("desc", td.description);
  }

  @Test
  void transformDefinition_copyConstructor_nullTransformMeta() {
    TransformDefinition original = new TransformDefinition("tag", "step", null, "d");
    TransformDefinition copy = new TransformDefinition(original);
    assertNotSame(original, copy);
    assertEquals(original.tag, copy.tag);
    assertEquals(original.transformName, copy.transformName);
    assertEquals(original.description, copy.description);
    assertNull(copy.transformMeta);
  }

  @Test
  void transformDefinition_clone_createsDistinctInstance() {
    TransformDefinition td = new TransformDefinition("a", "b", null, "c");
    TransformDefinition cloned = (TransformDefinition) td.clone();
    assertNotSame(td, cloned);
    assertEquals(td.tag, cloned.tag);
    assertEquals(td.transformName, cloned.transformName);
    assertEquals(td.description, cloned.description);
  }

  // ================================================================== InfoTransformDefinition

  @Test
  void infoTransformDefinition_defaultConstructor_emptyStrings() {
    InfoTransformDefinition itd = new InfoTransformDefinition();
    assertNotNull(itd);
    assertNull(itd.transformMeta);
  }

  @Test
  void infoTransformDefinition_copyConstructor_noTransformMeta() {
    InfoTransformDefinition original = new InfoTransformDefinition();
    original.setTag("info");
    original.setTransformName("src");
    original.setDescription("info desc");
    original.transformMeta = null;

    InfoTransformDefinition copy = new InfoTransformDefinition(original);
    assertNotSame(original, copy);
    assertEquals("info", copy.getTag());
    assertEquals("src", copy.getTransformName());
    assertEquals("info desc", copy.getDescription());
    assertNull(copy.transformMeta);
  }

  @Test
  void infoTransformDefinition_copyConstructor_withTransformMeta_clonesIt() {
    InfoTransformDefinition original = new InfoTransformDefinition();
    original.setTag("x");
    TransformMeta tm = mock(TransformMeta.class);
    original.transformMeta = tm;

    InfoTransformDefinition copy = new InfoTransformDefinition(original);
    // clone() should have been called — the field must not be the same reference
    assertNotSame(tm, copy.transformMeta);
  }

  @Test
  void infoTransformDefinition_clone_returnsDistinctInstance() {
    InfoTransformDefinition itd = new InfoTransformDefinition();
    itd.setTag("y");
    InfoTransformDefinition cloned = (InfoTransformDefinition) itd.clone();
    assertNotSame(itd, cloned);
    assertEquals("y", cloned.getTag());
  }

  // ================================================================== TargetTransformDefinition

  @Test
  void targetTransformDefinition_defaultConstructor_emptyStrings() {
    TargetTransformDefinition ttd = new TargetTransformDefinition();
    assertNotNull(ttd);
    assertNull(ttd.transformMeta);
  }

  @Test
  void targetTransformDefinition_copyConstructor_noTransformMeta() {
    TargetTransformDefinition original = new TargetTransformDefinition();
    original.tag = "tgt";
    original.transformName = "dest";
    original.description = "tgt desc";
    original.transformMeta = null;

    TargetTransformDefinition copy = new TargetTransformDefinition(original);
    assertNotSame(original, copy);
    assertEquals("tgt", copy.tag);
    assertEquals("dest", copy.transformName);
    assertEquals("tgt desc", copy.description);
    assertNull(copy.transformMeta);
  }

  @Test
  void targetTransformDefinition_copyConstructor_withTransformMeta_clonesIt() {
    TargetTransformDefinition original = new TargetTransformDefinition();
    original.tag = "z";
    TransformMeta tm = mock(TransformMeta.class);
    original.transformMeta = tm;

    TargetTransformDefinition copy = new TargetTransformDefinition(original);
    assertNotSame(tm, copy.transformMeta);
  }

  @Test
  void targetTransformDefinition_clone_returnsDistinctInstance() {
    TargetTransformDefinition ttd = new TargetTransformDefinition();
    ttd.tag = "alpha";
    TargetTransformDefinition cloned = (TargetTransformDefinition) ttd.clone();
    assertNotSame(ttd, cloned);
    assertEquals("alpha", cloned.tag);
  }

  // ================================================================== UsageParameter

  @Test
  void usageParameter_defaultConstructor_allNull() {
    UsageParameter up = new UsageParameter();
    assertNull(up.getTag());
    assertNull(up.getValue());
    assertNull(up.getDescription());
  }

  @Test
  void usageParameter_copyConstructor_copiesAllFields() {
    UsageParameter original = new UsageParameter();
    original.setTag("param1");
    original.setValue("val1");
    original.setDescription("a parameter");

    UsageParameter copy = new UsageParameter(original);
    assertNotSame(original, copy);
    assertEquals("param1", copy.getTag());
    assertEquals("val1", copy.getValue());
    assertEquals("a parameter", copy.getDescription());
  }

  @Test
  void usageParameter_clone_returnsDistinctInstance() {
    UsageParameter up = new UsageParameter();
    up.setTag("p");
    up.setValue("v");
    UsageParameter cloned = up.clone();
    assertNotSame(up, cloned);
    assertEquals("p", cloned.getTag());
    assertEquals("v", cloned.getValue());
  }

  @Test
  void usageParameter_setters_updateState() {
    UsageParameter up = new UsageParameter();
    up.setTag("t");
    up.setValue("42");
    up.setDescription("desc");
    assertEquals("t", up.getTag());
    assertEquals("42", up.getValue());
    assertEquals("desc", up.getDescription());
  }
}
