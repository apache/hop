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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaPlugin;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class UserDefinedJavaClassMetaTest {

  @Test
  void cookClassErrorCompilationTest() {
    String wrongCode =
        "public boolean processRow() {\n"
            + "   return true;\n"
            + "}\n"
            + "\n"
            + "public boolean processRow() {\n"
            + "   return true;\n"
            + "}\n";

    UserDefinedJavaClassMeta userDefinedJavaClassMeta = new UserDefinedJavaClassMeta();

    UserDefinedJavaClassDef userDefinedJavaClassDef = Mockito.mock(UserDefinedJavaClassDef.class);
    Mockito.when(userDefinedJavaClassDef.isTransformClass()).thenReturn(false);
    Mockito.when(userDefinedJavaClassDef.getSource()).thenReturn(wrongCode);
    Mockito.when(userDefinedJavaClassDef.getClassName()).thenReturn("MainClass");

    TransformMeta transformMeta = Mockito.mock(TransformMeta.class);
    Mockito.when(transformMeta.getName()).thenReturn("User Defined Java Class");
    userDefinedJavaClassMeta.setParentTransformMeta(transformMeta);

    UserDefinedJavaClassMeta userDefinedJavaClassMetaSpy = Mockito.spy(userDefinedJavaClassMeta);
    Mockito.when(userDefinedJavaClassMetaSpy.getDefinitions())
        .thenReturn(Collections.singletonList(userDefinedJavaClassDef));

    try {
      userDefinedJavaClassMetaSpy.cookClasses();
    } catch (HopException e) {
      throw new RuntimeException(e);
    }

    assertEquals(1, userDefinedJavaClassMeta.getCookErrors().size());
  }

  @Test
  void cookClassesCachingTest() throws Exception {
    String codeBlock1 = "public boolean processRow() {\n" + "    return true;\n" + "}\n\n";
    String codeBlock2 =
        "public boolean processRow() {\n"
            + "    // Random comment\n"
            + "    return true;\n"
            + "}\n\n";
    UserDefinedJavaClassMeta userDefinedJavaClassMeta1 = new UserDefinedJavaClassMeta();

    UserDefinedJavaClassDef userDefinedJavaClassDef1 =
        new UserDefinedJavaClassDef(
            UserDefinedJavaClassDef.ClassType.NORMAL_CLASS, "MainClass", codeBlock1);

    TransformMeta transformMeta = Mockito.mock(TransformMeta.class);
    Mockito.when(transformMeta.getName()).thenReturn("User Defined Java Class");
    userDefinedJavaClassMeta1.setParentTransformMeta(transformMeta);

    UserDefinedJavaClassMeta userDefinedJavaClassMetaSpy = Mockito.spy(userDefinedJavaClassMeta1);

    // Added classloader
    Class<?> clazz1 = userDefinedJavaClassMetaSpy.cookClass(userDefinedJavaClassDef1, null);
    Class<?> clazz2 =
        userDefinedJavaClassMetaSpy.cookClass(userDefinedJavaClassDef1, clazz1.getClassLoader());
    assertSame(clazz1, clazz2); // Caching should work here and return exact same class

    UserDefinedJavaClassMeta userDefinedJavaClassMeta2 = new UserDefinedJavaClassMeta();
    UserDefinedJavaClassDef userDefinedJavaClassDef2 =
        new UserDefinedJavaClassDef(
            UserDefinedJavaClassDef.ClassType.NORMAL_CLASS, "AnotherClass", codeBlock2);

    TransformMeta transformMeta2 = Mockito.mock(TransformMeta.class);
    Mockito.when(transformMeta2.getName()).thenReturn("Another UDJC");
    userDefinedJavaClassMeta2.setParentTransformMeta(transformMeta2);
    UserDefinedJavaClassMeta userDefinedJavaClassMeta2Spy = Mockito.spy(userDefinedJavaClassMeta2);

    Class<?> clazz3 =
        userDefinedJavaClassMeta2Spy.cookClass(userDefinedJavaClassDef2, clazz2.getClassLoader());

    assertNotSame(clazz3, clazz1); // They should not be the exact same class
  }

  @Test
  void oderDefinitionTest() {
    String codeBlock1 = "public boolean processRow() {\n" + "    return true;\n" + "}\n\n";
    UserDefinedJavaClassMeta userDefinedJavaClassMeta = new UserDefinedJavaClassMeta();
    UserDefinedJavaClassDef processClassDef =
        new UserDefinedJavaClassDef(
            UserDefinedJavaClassDef.ClassType.TRANSFORM_CLASS, "Process", codeBlock1);
    UserDefinedJavaClassDef processClassDefA =
        new UserDefinedJavaClassDef(
            UserDefinedJavaClassDef.ClassType.TRANSFORM_CLASS, "ProcessA", codeBlock1);
    UserDefinedJavaClassDef normalClassADef =
        new UserDefinedJavaClassDef(
            UserDefinedJavaClassDef.ClassType.NORMAL_CLASS, "A", codeBlock1);
    UserDefinedJavaClassDef normalClassBDef =
        new UserDefinedJavaClassDef(
            UserDefinedJavaClassDef.ClassType.NORMAL_CLASS, "B", codeBlock1);
    UserDefinedJavaClassDef normalClassCDef =
        new UserDefinedJavaClassDef(
            UserDefinedJavaClassDef.ClassType.NORMAL_CLASS, "C", codeBlock1);

    ArrayList<UserDefinedJavaClassDef> defs = new ArrayList<>(5);
    defs.add(processClassDefA);
    defs.add(processClassDef);
    defs.add(normalClassCDef);
    defs.add(normalClassBDef);
    defs.add(normalClassADef);

    TransformMeta transformMeta = Mockito.mock(TransformMeta.class);
    Mockito.when(transformMeta.getName()).thenReturn("User Defined Java Class");
    userDefinedJavaClassMeta.setParentTransformMeta(transformMeta);

    // Test reording the reverse order test
    List<UserDefinedJavaClassDef> orderDefs = userDefinedJavaClassMeta.orderDefinitions(defs);
    assertEquals("A", orderDefs.get(0).getClassName());
    assertEquals("B", orderDefs.get(1).getClassName());
    assertEquals("C", orderDefs.get(2).getClassName());
    assertEquals("Process", orderDefs.get(3).getClassName());
    assertEquals("ProcessA", orderDefs.get(4).getClassName());

    // Random order test
    defs.clear();
    defs.add(normalClassADef);
    defs.add(normalClassCDef);
    defs.add(processClassDefA);
    defs.add(normalClassBDef);
    defs.add(processClassDef);
    orderDefs = userDefinedJavaClassMeta.orderDefinitions(defs);
    assertEquals("A", orderDefs.get(0).getClassName());
    assertEquals("B", orderDefs.get(1).getClassName());
    assertEquals("C", orderDefs.get(2).getClassName());
    assertEquals("Process", orderDefs.get(3).getClassName());
    assertEquals("ProcessA", orderDefs.get(4).getClassName());
  }

  void minimalPluginPreparation() throws Exception {
    PluginRegistry registry = PluginRegistry.getInstance();
    String[] classNames = {
      ValueMetaString.class.getName(), ValueMetaInteger.class.getName(),
      ValueMetaDate.class.getName(), ValueMetaNumber.class.getName()
    };
    for (String className : classNames) {
      registry.registerPluginClass(className, ValueMetaPluginType.class, ValueMetaPlugin.class);
    }
  }

  @Test
  void testRoundTrip() throws Exception {
    minimalPluginPreparation();
    UserDefinedJavaClassMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/user-defined-java-class.xml", UserDefinedJavaClassMeta.class);
    validate(meta);
  }

  private void validate(UserDefinedJavaClassMeta meta) {
    assertFalse(meta.isClearingResultFields());

    // Definitions
    assertEquals(2, meta.getDefinitions().size());
    UserDefinedJavaClassDef d = meta.getDefinitions().getFirst();
    assertEquals(UserDefinedJavaClassDef.ClassType.NORMAL_CLASS, d.getClassType());
    assertEquals("Processor2", d.getClassName());
    assertEquals("processor2", d.getSource());

    d = meta.getDefinitions().getLast();
    assertEquals(UserDefinedJavaClassDef.ClassType.TRANSFORM_CLASS, d.getClassType());
    assertEquals("Processor", d.getClassName());
    assertEquals("processor1", d.getSource());

    // Fields
    assertEquals(3, meta.getFields().size());
    UserDefinedJavaClassMeta.FieldInfo f = meta.getFields().getFirst();
    assertEquals("f1", f.getName());
    assertEquals(IValueMeta.TYPE_STRING, f.getType());
    assertEquals(100, f.getLength());
    assertEquals(-1, f.getPrecision());

    f = meta.getFields().get(1);
    assertEquals("f2", f.getName());
    assertEquals(IValueMeta.TYPE_INTEGER, f.getType());
    assertEquals(7, f.getLength());
    assertEquals(-1, f.getPrecision());

    f = meta.getFields().getLast();
    assertEquals("f3", f.getName());
    assertEquals(IValueMeta.TYPE_NUMBER, f.getType());
    assertEquals(9, f.getLength());
    assertEquals(2, f.getPrecision());

    // Info transforms
    assertEquals(2, meta.getInfoTransformDefinitions().size());
    InfoTransformDefinition i = meta.getInfoTransformDefinitions().getFirst();
    assertEquals("info1", i.getTag());
    assertEquals("infoTransform1", i.getTransformName());
    assertEquals("reads from transform1", i.getDescription());

    i = meta.getInfoTransformDefinitions().getLast();
    assertEquals("info2", i.getTag());
    assertEquals("infoTransform2", i.getTransformName());
    assertEquals("reads from transform2", i.getDescription());

    assertEquals(2, meta.getTargetTransformDefinitions().size());
    TargetTransformDefinition t = meta.getTargetTransformDefinitions().getFirst();
    assertEquals("target1", t.getTag());
    assertEquals("targetTransform1", t.getTransformName());
    assertEquals("targets transform 1", t.getDescription());

    t = meta.getTargetTransformDefinitions().getLast();
    assertEquals("target2", t.getTag());
    assertEquals("targetTransform2", t.getTransformName());
    assertEquals("targets transform 2", t.getDescription());

    // Parameters
    assertEquals(2, meta.getUsageParameters().size());
    UsageParameter p = meta.getUsageParameters().getFirst();
    assertEquals("PARAM1", p.getTag());
    assertEquals("parameterValue1", p.getValue());
    assertEquals("parameterDescription1", p.getDescription());

    p = meta.getUsageParameters().getLast();
    assertEquals("PARAM2", p.getTag());
    assertEquals("parameterValue2", p.getValue());
    assertEquals("parameterDescription2", p.getDescription());
  }
}
