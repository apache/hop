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
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Collections;
import java.util.List;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaPlugin;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.pipeline.transforms.userdefinedjavaclass.UserDefinedJavaClassMeta.FieldInfo;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests the static utility methods on {@link TransformClassBase} that do not require a running
 * transform instance: {@code getInfoTransforms}, {@code getFields}, and {@code getTransformIOMeta}.
 */
class TransformClassBaseStaticMethodsTest {

  @BeforeAll
  static void initPlugins() throws Exception {
    PluginRegistry registry = PluginRegistry.getInstance();
    registry.registerPluginClass(
        ValueMetaString.class.getName(), ValueMetaPluginType.class, ValueMetaPlugin.class);
  }

  // ------------------------------------------------------------------ getInfoTransforms

  @Test
  void getInfoTransforms_returnsNull() {
    assertNull(TransformClassBase.getInfoTransforms());
  }

  // ------------------------------------------------------------------ getFields

  @Test
  void getFields_appendMode_addsFieldsWithoutClearing() throws Exception {
    RowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("existing"));

    FieldInfo fi = new FieldInfo("newCol", IValueMeta.TYPE_STRING, 50, 0);

    TransformClassBase.getFields(false, row, "origin", null, null, null, List.of(fi));

    assertEquals(2, row.size());
    assertEquals("existing", row.getValueMeta(0).getName());
    assertEquals("newCol", row.getValueMeta(1).getName());
  }

  @Test
  void getFields_clearMode_clearsBeforeAdding() throws Exception {
    RowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("toBeCleared"));

    FieldInfo fi = new FieldInfo("fresh", IValueMeta.TYPE_STRING, 10, 0);

    TransformClassBase.getFields(true, row, "origin", null, null, null, List.of(fi));

    assertEquals(1, row.size());
    assertEquals("fresh", row.getValueMeta(0).getName());
  }

  @Test
  void getFields_emptyList_leavesRowUnchanged() throws Exception {
    RowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("keep"));

    TransformClassBase.getFields(false, row, "origin", null, null, null, Collections.emptyList());

    assertEquals(1, row.size());
    assertEquals("keep", row.getValueMeta(0).getName());
  }

  @Test
  void getFields_clearWithEmptyList_producesEmptyRow() throws Exception {
    RowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("gone"));

    TransformClassBase.getFields(true, row, "origin", null, null, null, Collections.emptyList());

    assertEquals(0, row.size());
  }

  @Test
  void getFields_setsOriginOnAddedField() throws Exception {
    RowMeta row = new RowMeta();
    FieldInfo fi = new FieldInfo("col", IValueMeta.TYPE_STRING, 5, 0);

    TransformClassBase.getFields(false, row, "MyTransform", null, null, null, List.of(fi));

    assertEquals("MyTransform", row.getValueMeta(0).getOrigin());
  }

  // ------------------------------------------------------------------ getTransformIOMeta

  @Test
  void getTransformIOMeta_noInfoNoTarget_returnsEmptyStreams() {
    UserDefinedJavaClassMeta meta = new UserDefinedJavaClassMeta();

    ITransformIOMeta ioMeta = TransformClassBase.getTransformIOMeta(meta);

    assertNotNull(ioMeta);
    long infoStreams =
        ioMeta.getInfoStreams().stream()
            .filter(s -> s.getStreamType() == IStream.StreamType.INFO)
            .count();
    long targetStreams =
        ioMeta.getTargetStreams().stream()
            .filter(s -> s.getStreamType() == IStream.StreamType.TARGET)
            .count();
    assertEquals(0, infoStreams);
    assertEquals(0, targetStreams);
  }

  @Test
  void getTransformIOMeta_withInfoDefinition_includesInfoStream() {
    UserDefinedJavaClassMeta meta = new UserDefinedJavaClassMeta();
    InfoTransformDefinition infoDef = new InfoTransformDefinition();
    infoDef.setTag("lookup");
    infoDef.setDescription("Lookup data");
    meta.getInfoTransformDefinitions().add(infoDef);

    ITransformIOMeta ioMeta = TransformClassBase.getTransformIOMeta(meta);

    IRowMeta infoStreams = null; // unused, just verify count
    long count =
        ioMeta.getInfoStreams().stream()
            .filter(s -> s.getStreamType() == IStream.StreamType.INFO)
            .count();
    assertEquals(1, count);
  }

  @Test
  void getTransformIOMeta_withTargetDefinition_includesTargetStream() {
    UserDefinedJavaClassMeta meta = new UserDefinedJavaClassMeta();
    TargetTransformDefinition targetDef = new TargetTransformDefinition();
    targetDef.tag = "positive";
    targetDef.description = "Positive rows";
    meta.getTargetTransformDefinitions().add(targetDef);

    ITransformIOMeta ioMeta = TransformClassBase.getTransformIOMeta(meta);

    long count =
        ioMeta.getTargetStreams().stream()
            .filter(s -> s.getStreamType() == IStream.StreamType.TARGET)
            .count();
    assertEquals(1, count);
  }
}
