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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.transform.TransformMeta;
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
    Mockito.when(userDefinedJavaClassDef.isActive()).thenReturn(true);

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

    assertEquals(1, userDefinedJavaClassMeta.cookErrors.size());
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
}
