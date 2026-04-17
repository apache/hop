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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaPlugin;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.apache.hop.pipeline.transforms.janino.JaninoMeta;
import org.codehaus.commons.compiler.CompileException;
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
      throw new HopRuntimeException(e);
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
    assertEquals(17, meta.getJavaTargetVersion());
    assertEquals(17, meta.getEffectiveJavaTargetVersion());

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

  // ------------------------------------------------------------------
  // getEffectiveJavaTargetVersion

  @Test
  void effectiveVersion_default_returnsDefault() {
    UserDefinedJavaClassMeta meta = new UserDefinedJavaClassMeta();
    assertEquals(JaninoMeta.JAVA_TARGET_VERSION_DEFAULT, meta.getEffectiveJavaTargetVersion());
  }

  @Test
  void effectiveVersion_validValues_returnAsIs() {
    UserDefinedJavaClassMeta meta = new UserDefinedJavaClassMeta();
    meta.setJavaTargetVersion(8);
    assertEquals(8, meta.getEffectiveJavaTargetVersion());
    meta.setJavaTargetVersion(11);
    assertEquals(11, meta.getEffectiveJavaTargetVersion());
    meta.setJavaTargetVersion(JaninoMeta.JAVA_TARGET_VERSION_MIN);
    assertEquals(JaninoMeta.JAVA_TARGET_VERSION_MIN, meta.getEffectiveJavaTargetVersion());
    meta.setJavaTargetVersion(JaninoMeta.JAVA_TARGET_VERSION_MAX);
    assertEquals(JaninoMeta.JAVA_TARGET_VERSION_MAX, meta.getEffectiveJavaTargetVersion());
  }

  @Test
  void effectiveVersion_tooLow_returnsDefault() {
    UserDefinedJavaClassMeta meta = new UserDefinedJavaClassMeta();
    meta.setJavaTargetVersion(0);
    assertEquals(JaninoMeta.JAVA_TARGET_VERSION_DEFAULT, meta.getEffectiveJavaTargetVersion());
    meta.setJavaTargetVersion(-5);
    assertEquals(JaninoMeta.JAVA_TARGET_VERSION_DEFAULT, meta.getEffectiveJavaTargetVersion());
  }

  @Test
  void effectiveVersion_tooHigh_returnsDefault() {
    UserDefinedJavaClassMeta meta = new UserDefinedJavaClassMeta();
    meta.setJavaTargetVersion(99);
    assertEquals(JaninoMeta.JAVA_TARGET_VERSION_DEFAULT, meta.getEffectiveJavaTargetVersion());
  }

  // ------------------------------------------------------------------ setJavaTargetVersion marks
  // hasChanged

  @Test
  void setJavaTargetVersion_differentValue_marksHasChanged() throws HopException {
    String code =
        "public boolean processRow() throws HopException { setOutputDone(); return false; }\n";
    UserDefinedJavaClassMeta meta = new UserDefinedJavaClassMeta();
    // Cook once so hasChanged becomes false
    UserDefinedJavaClassDef def =
        new UserDefinedJavaClassDef(
            UserDefinedJavaClassDef.ClassType.TRANSFORM_CLASS, "Proc", code);
    meta.replaceDefinitions(new ArrayList<>(Collections.singletonList(def)));
    meta.cookClasses();

    // Now change the version — should set hasChanged
    meta.setJavaTargetVersion(8);
    assertEquals(8, meta.getJavaTargetVersion());
  }

  @Test
  void setJavaTargetVersion_sameValue_doesNotAlterValue() {
    UserDefinedJavaClassMeta meta = new UserDefinedJavaClassMeta();
    int initial = meta.getJavaTargetVersion();
    meta.setJavaTargetVersion(initial); // same value
    assertEquals(initial, meta.getJavaTargetVersion());
  }

  // ------------------------------------------------------------------ clone copies version

  @Test
  void clone_copiesJavaTargetVersion() {
    UserDefinedJavaClassMeta original = new UserDefinedJavaClassMeta();
    original.setJavaTargetVersion(17);
    UserDefinedJavaClassMeta copy = original.clone();

    assertNotSame(original, copy);
    assertEquals(17, copy.getJavaTargetVersion());
    assertEquals(17, copy.getEffectiveJavaTargetVersion());
  }

  // ------------------------------------------------------------------ cookClass: different target
  // versions

  @Test
  void cookClass_target8_staticInterfaceMethod_succeeds() throws Exception {
    String code =
        "public int cmp() {\n"
            + "  return java.util.Comparator.naturalOrder().compare(Integer.valueOf(3), Integer.valueOf(5));\n"
            + "}\n";
    UserDefinedJavaClassMeta meta = new UserDefinedJavaClassMeta();
    meta.setJavaTargetVersion(8);
    UserDefinedJavaClassDef def =
        new UserDefinedJavaClassDef(
            UserDefinedJavaClassDef.ClassType.NORMAL_CLASS, "CmpClass", code);

    Class<?> cooked = meta.cookClass(def, null);
    assertNotSame(null, cooked);
    assertEquals("CmpClass", cooked.getSimpleName());
  }

  @Test
  void cookClass_target17_staticInterfaceMethod_succeeds() throws Exception {
    String code =
        "public int cmp() {\n"
            + "  return java.util.Comparator.naturalOrder().compare(Integer.valueOf(3), Integer.valueOf(5));\n"
            + "}\n";
    UserDefinedJavaClassMeta meta = new UserDefinedJavaClassMeta();
    meta.setJavaTargetVersion(17);
    UserDefinedJavaClassDef def =
        new UserDefinedJavaClassDef(
            UserDefinedJavaClassDef.ClassType.NORMAL_CLASS, "CmpClass17", code);

    Class<?> cooked = meta.cookClass(def, null);
    assertNotSame(null, cooked);
  }

  @Test
  void cookClass_target6_staticInterfaceMethod_throwsCompileException() {
    String code =
        "public int cmp() {\n"
            + "  return java.util.Comparator.naturalOrder().compare(Integer.valueOf(3), Integer.valueOf(5));\n"
            + "}\n";
    UserDefinedJavaClassMeta meta = new UserDefinedJavaClassMeta();
    meta.setJavaTargetVersion(6);
    UserDefinedJavaClassDef def =
        new UserDefinedJavaClassDef(
            UserDefinedJavaClassDef.ClassType.NORMAL_CLASS, "CmpClass6", code);

    assertThrows(CompileException.class, () -> meta.cookClass(def, null));
  }

  // ------------------------------------------------------------------ cache: different versions
  // use separate entries

  @Test
  void cookClass_sameCodeDifferentTargets_returnsDifferentClasses() throws Exception {
    String code = "public int val() { return 1; }\n";

    UserDefinedJavaClassMeta meta8 = new UserDefinedJavaClassMeta();
    meta8.setJavaTargetVersion(8);
    UserDefinedJavaClassDef def8 =
        new UserDefinedJavaClassDef(
            UserDefinedJavaClassDef.ClassType.NORMAL_CLASS, "ValClass", code);

    UserDefinedJavaClassMeta meta17 = new UserDefinedJavaClassMeta();
    meta17.setJavaTargetVersion(17);
    UserDefinedJavaClassDef def17 =
        new UserDefinedJavaClassDef(
            UserDefinedJavaClassDef.ClassType.NORMAL_CLASS, "ValClass", code);

    Class<?> cooked8 = meta8.cookClass(def8, null);
    Class<?> cooked17 = meta17.cookClass(def17, null);
    // Both succeed; they must be non-null
    assertNotSame(null, cooked8);
    assertNotSame(null, cooked17);
  }

  // ------------------------------------------------------------------ check()

  @Test
  void check_withInputTransforms_addsOk() {
    UserDefinedJavaClassMeta meta = new UserDefinedJavaClassMeta();
    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        new RowMeta(),
        new String[] {"in"},
        new String[0],
        mock(IRowMeta.class),
        null,
        mock(IHopMetadataProvider.class));

    assertTrue(remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_OK));
  }

  @Test
  void check_noInputTransforms_addsError() {
    UserDefinedJavaClassMeta meta = new UserDefinedJavaClassMeta();
    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        mock(PipelineMeta.class),
        mock(TransformMeta.class),
        new RowMeta(),
        new String[0],
        new String[0],
        mock(IRowMeta.class),
        null,
        mock(IHopMetadataProvider.class));

    assertTrue(remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR));
  }

  // ------------------------------------------------------------------ supportsErrorHandling /
  // excludeFromRowLayoutVerification

  @Test
  void supportsErrorHandling_returnsTrue() {
    assertTrue(new UserDefinedJavaClassMeta().supportsErrorHandling());
  }

  @Test
  void excludeFromRowLayoutVerification_returnsTrue() {
    assertTrue(new UserDefinedJavaClassMeta().excludeFromRowLayoutVerification());
  }

  // ------------------------------------------------------------------ FieldInfo

  @Test
  void fieldInfo_constructorAndGetters() {
    UserDefinedJavaClassMeta.FieldInfo f =
        new UserDefinedJavaClassMeta.FieldInfo("myField", IValueMeta.TYPE_INTEGER, 9, 2);
    assertEquals("myField", f.getName());
    assertEquals(IValueMeta.TYPE_INTEGER, f.getType());
    assertEquals(9, f.getLength());
    assertEquals(2, f.getPrecision());
  }

  @Test
  void fieldInfo_copyConstructor_copiesAllFields() {
    UserDefinedJavaClassMeta.FieldInfo original =
        new UserDefinedJavaClassMeta.FieldInfo("x", IValueMeta.TYPE_STRING, 50, -1);
    UserDefinedJavaClassMeta.FieldInfo copy = new UserDefinedJavaClassMeta.FieldInfo(original);
    assertNotSame(original, copy);
    assertEquals("x", copy.getName());
    assertEquals(IValueMeta.TYPE_STRING, copy.getType());
    assertEquals(50, copy.getLength());
    assertEquals(-1, copy.getPrecision());
  }

  @Test
  void fieldInfo_setters() {
    UserDefinedJavaClassMeta.FieldInfo f = new UserDefinedJavaClassMeta.FieldInfo();
    f.setName("n");
    f.setType(IValueMeta.TYPE_NUMBER);
    f.setLength(10);
    f.setPrecision(3);
    assertEquals("n", f.getName());
    assertEquals(IValueMeta.TYPE_NUMBER, f.getType());
    assertEquals(10, f.getLength());
    assertEquals(3, f.getPrecision());
  }

  // ------------------------------------------------------------------ replaceFields / setFieldInfo

  @Test
  void replaceFields_updatesFieldsList() {
    UserDefinedJavaClassMeta meta = new UserDefinedJavaClassMeta();
    List<UserDefinedJavaClassMeta.FieldInfo> newFields = new ArrayList<>();
    newFields.add(new UserDefinedJavaClassMeta.FieldInfo("a", IValueMeta.TYPE_STRING, 10, -1));
    meta.replaceFields(newFields);
    assertEquals(1, meta.getFields().size());
    assertEquals("a", meta.getFields().get(0).getName());
  }

  @Test
  void setFieldInfo_delegatesToReplaceFields() {
    UserDefinedJavaClassMeta meta = new UserDefinedJavaClassMeta();
    List<UserDefinedJavaClassMeta.FieldInfo> fields = new ArrayList<>();
    fields.add(new UserDefinedJavaClassMeta.FieldInfo("b", IValueMeta.TYPE_INTEGER, 9, 0));
    meta.setFieldInfo(fields);
    assertEquals(1, meta.getFields().size());
    assertEquals("b", meta.getFields().get(0).getName());
  }

  // ------------------------------------------------------------------ replaceDefinitions

  @Test
  void replaceDefinitions_ordersNormalBeforeTransformClasses() {
    UserDefinedJavaClassMeta meta = new UserDefinedJavaClassMeta();
    List<UserDefinedJavaClassDef> defs = new ArrayList<>();
    defs.add(
        new UserDefinedJavaClassDef(
            UserDefinedJavaClassDef.ClassType.TRANSFORM_CLASS,
            "Proc",
            "public boolean processRow() { return true; }\n"));
    defs.add(
        new UserDefinedJavaClassDef(
            UserDefinedJavaClassDef.ClassType.NORMAL_CLASS,
            "Helper",
            "public int x() { return 1; }\n"));
    meta.replaceDefinitions(defs);

    // Normal classes come first
    assertEquals("Helper", meta.getDefinitions().get(0).getClassName());
    assertEquals("Proc", meta.getDefinitions().get(1).getClassName());
  }

  // ------------------------------------------------------------------
  // searchInfoAndTargetTransforms

  @Test
  void searchInfoAndTargetTransforms_resolvesMatchingTransforms() {
    UserDefinedJavaClassMeta meta = new UserDefinedJavaClassMeta();

    InfoTransformDefinition infoDef = new InfoTransformDefinition();
    infoDef.setTag("lookup");
    infoDef.setTransformName("LookupStep");
    meta.getInfoTransformDefinitions().add(infoDef);

    TargetTransformDefinition targetDef = new TargetTransformDefinition();
    targetDef.tag = "out";
    targetDef.transformName = "OutputStep";
    meta.getTargetTransformDefinitions().add(targetDef);

    TransformMeta lookupMeta = Mockito.mock(TransformMeta.class);
    Mockito.when(lookupMeta.getName()).thenReturn("LookupStep");
    TransformMeta outputMeta = Mockito.mock(TransformMeta.class);
    Mockito.when(outputMeta.getName()).thenReturn("OutputStep");

    meta.searchInfoAndTargetTransforms(List.of(lookupMeta, outputMeta));

    assertEquals(lookupMeta, infoDef.transformMeta);
    assertEquals(outputMeta, targetDef.transformMeta);
  }

  @Test
  void searchInfoAndTargetTransforms_noMatchSetsNull() {
    UserDefinedJavaClassMeta meta = new UserDefinedJavaClassMeta();

    InfoTransformDefinition infoDef = new InfoTransformDefinition();
    infoDef.setTag("lookup");
    infoDef.setTransformName("NonExistent");
    meta.getInfoTransformDefinitions().add(infoDef);

    TransformMeta otherMeta = Mockito.mock(TransformMeta.class);
    Mockito.when(otherMeta.getName()).thenReturn("SomeOtherStep");

    meta.searchInfoAndTargetTransforms(List.of(otherMeta));

    assertNull(infoDef.transformMeta);
  }

  // ------------------------------------------------------------------ getFields: cook errors

  @Test
  void getFields_withCookErrors_throwsHopTransformException() throws Exception {
    UserDefinedJavaClassMeta meta = new UserDefinedJavaClassMeta();
    UserDefinedJavaClassDef badDef = Mockito.mock(UserDefinedJavaClassDef.class);
    Mockito.when(badDef.isTransformClass()).thenReturn(false);
    Mockito.when(badDef.getSource()).thenReturn("THIS IS NOT JAVA !!!");
    Mockito.when(badDef.getClassName()).thenReturn("Bad");
    // getChecksum() throws HopTransformException – use doReturn to avoid the checked-exception
    // surfacing in the when() call itself
    Mockito.doReturn("badchecksum-unique-" + System.nanoTime()).when(badDef).getChecksum();

    UserDefinedJavaClassMeta spy = Mockito.spy(meta);
    Mockito.when(spy.getDefinitions()).thenReturn(Collections.singletonList(badDef));

    TransformMeta transformMeta = Mockito.mock(TransformMeta.class);
    Mockito.when(transformMeta.getName()).thenReturn("UDJC");
    spy.setParentTransformMeta(transformMeta);

    assertThrows(
        HopTransformException.class,
        () -> spy.getFields(new RowMeta(), "step", null, null, null, null));
  }
}
