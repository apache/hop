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

package org.apache.hop.ui.core.gui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

/**
 * Verifies static toolbar listener resolution accepts interface/superclass parameters (facade
 * pattern used by the content editor toolbar).
 */
class BaseGuiWidgetsStaticListenerTest {

  interface EditorFacade {
    String id();
  }

  static final class ConcreteEditor implements EditorFacade {
    @Override
    public String id() {
      return "concrete";
    }
  }

  static final class ListenerHost {
    static EditorFacade lastFacade;
    static ConcreteEditor lastConcrete;

    public static void onFacade(EditorFacade editor) {
      lastFacade = editor;
    }

    public static void onConcrete(ConcreteEditor editor) {
      lastConcrete = editor;
    }

    public static void ignored(String other) {
      // not a match for EditorFacade argument
    }
  }

  @Test
  void findsStaticMethodWithInterfaceParameter() throws Exception {
    Method method =
        BaseGuiWidgets.findStaticListenerMethod(
            ListenerHost.class, "onFacade", ConcreteEditor.class);
    assertNotNull(method);
    assertEquals("onFacade", method.getName());

    ConcreteEditor editor = new ConcreteEditor();
    method.invoke(null, editor);
    assertSame(editor, ListenerHost.lastFacade);
  }

  @Test
  void prefersMostSpecificParameterType() {
    Method method =
        BaseGuiWidgets.findStaticListenerMethod(
            ListenerHost.class, "onConcrete", ConcreteEditor.class);
    assertNotNull(method);
    assertEquals(ConcreteEditor.class, method.getParameterTypes()[0]);
  }

  @Test
  void returnsNullWhenNoAssignableParameter() {
    Method method =
        BaseGuiWidgets.findStaticListenerMethod(
            ListenerHost.class, "ignored", ConcreteEditor.class);
    assertNull(method);
  }

  /** Toolbar methods live on the base widget. A subclass still has to find them. */
  public static class SelectionLine {
    public void edit() {}

    public void edit(String event) {}
  }

  public static class ConnectionSelectionLine extends SelectionLine {}

  public static class OverridingSelectionLine extends SelectionLine {
    @Override
    public void edit() {}
  }

  @Test
  void findsListenerMethodDeclaredOnASuperclass() throws Exception {
    Method method = BaseGuiWidgets.findDeclaredMethod(ConnectionSelectionLine.class, "edit");
    assertNotNull(method);
    assertEquals(SelectionLine.class, method.getDeclaringClass());
    assertEquals(0, method.getParameterCount());
  }

  @Test
  void prefersTheListenerMethodDeclaredOnTheInstanceClass() {
    Method method = BaseGuiWidgets.findDeclaredMethod(OverridingSelectionLine.class, "edit");
    assertNotNull(method);
    assertEquals(OverridingSelectionLine.class, method.getDeclaringClass());
  }

  @Test
  void findsTheEventListenerBeforeSearchingFurther() {
    Method method =
        BaseGuiWidgets.findDeclaredMethod(ConnectionSelectionLine.class, "edit", String.class);
    assertNotNull(method);
    assertEquals(String.class, method.getParameterTypes()[0]);
  }

  @Test
  void returnsNullWhenNoListenerMethodExists() {
    assertNull(BaseGuiWidgets.findDeclaredMethod(ConnectionSelectionLine.class, "missing"));
  }
}
