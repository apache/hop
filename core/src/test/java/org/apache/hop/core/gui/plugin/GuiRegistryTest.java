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

package org.apache.hop.core.gui.plugin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GuiRegistryTest {
  private GuiRegistry registry;

  @BeforeEach
  void before() {
    registry = GuiRegistry.getInstance();
  }

  @Test
  void retrieveClassInstance() {
    Object object1 = new Object();
    registry.registerGuiPluginObject("hop-gui-id1", "class1", "instance1", object1);
    Object verifyObject1111 = registry.findGuiPluginObject("hop-gui-id1", "class1", "instance1");
    assertEquals(object1, verifyObject1111);
    // class2 is not found
    Object verifyObject1211 = registry.findGuiPluginObject("hop-gui-id1", "class2", "instance1");
    assertNull(verifyObject1211);
  }

  @Test
  void retrieveSameObjectMultipleInstances() {
    Object object1 = new Object();
    registry.registerGuiPluginObject("hop-gui-id1", "class1", "instance1", object1);
    registry.registerGuiPluginObject("hop-gui-id1", "class1", "instance2", object1);

    Object verifyObject1121 = registry.findGuiPluginObject("hop-gui-id1", "class1", "instance2");
    assertEquals(object1, verifyObject1121);
    Object verifyObject1111 = registry.findGuiPluginObject("hop-gui-id1", "class1", "instance1");
    assertEquals(object1, verifyObject1111);
  }

  @Test
  void registeringAFieldWidgetTwiceKeepsOneElement() throws Exception {
    String dataClassName = getClass().getName() + "#fieldTwice";
    Field field = WidgetSample.class.getDeclaredField("name");
    GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);

    registry.addGuiWidgetElement(dataClassName, element, field);
    registry.addGuiWidgetElement(dataClassName, element, field);

    GuiElements elements = registry.findGuiElements(dataClassName, WidgetSample.PARENT_ID);
    assertEquals(1, elements.getChildren().size());
  }

  @Test
  void registeringAWidgetWithoutAnExplicitIdTwiceKeepsOneElement() throws Exception {
    // Without an id in the annotation the element takes the field name as its id.
    String dataClassName = getClass().getName() + "#noIdTwice";
    Field field = WidgetSample.class.getDeclaredField("sampleSize");
    GuiWidgetElement element = field.getAnnotation(GuiWidgetElement.class);

    registry.addGuiWidgetElement(dataClassName, element, field);
    registry.addGuiWidgetElement(dataClassName, element, field);

    GuiElements elements = registry.findGuiElements(dataClassName, WidgetSample.PARENT_ID);
    assertEquals(1, elements.getChildren().size());
    assertEquals("sampleSize", elements.getChildren().get(0).getId());
  }

  @Test
  void registeringAMethodWidgetTwiceKeepsOneElement() throws Exception {
    String dataClassName = getClass().getName() + "#methodTwice";
    Method method = WidgetSample.class.getDeclaredMethod("browse");
    GuiWidgetElement element = method.getAnnotation(GuiWidgetElement.class);
    ClassLoader classLoader = getClass().getClassLoader();

    registry.addGuiWidgetElement(element, method, dataClassName, classLoader);
    registry.addGuiWidgetElement(element, method, dataClassName, classLoader);

    GuiElements elements = registry.findGuiElements(dataClassName, WidgetSample.PARENT_ID);
    assertEquals(1, elements.getChildren().size());
  }

  @Test
  void anIgnoredDeclarationStillHidesARegisteredWidget() throws Exception {
    String dataClassName = getClass().getName() + "#ignored";
    Field field = WidgetSample.class.getDeclaredField("name");
    Field ignoredField = WidgetSample.class.getDeclaredField("hiddenName");

    registry.addGuiWidgetElement(dataClassName, field.getAnnotation(GuiWidgetElement.class), field);
    registry.addGuiWidgetElement(
        dataClassName, ignoredField.getAnnotation(GuiWidgetElement.class), ignoredField);

    GuiElements elements = registry.findGuiElements(dataClassName, WidgetSample.PARENT_ID);
    assertEquals(1, elements.getChildren().size());
    assertTrue(elements.getChildren().get(0).isIgnored());
  }

  private static class WidgetSample {
    static final String PARENT_ID = "GuiRegistryTest-parent";

    @GuiWidgetElement(id = "name", type = GuiElementType.TEXT, parentId = PARENT_ID)
    private String name;

    @GuiWidgetElement(id = "name", type = GuiElementType.TEXT, parentId = PARENT_ID, ignored = true)
    private String hiddenName;

    @GuiWidgetElement(type = GuiElementType.TEXT, parentId = PARENT_ID)
    private String sampleSize;

    @GuiWidgetElement(id = "browse", type = GuiElementType.BUTTON, parentId = PARENT_ID)
    void browse() {
      // Only the annotation matters here.
    }
  }
}
