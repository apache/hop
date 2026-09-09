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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import org.junit.jupiter.api.Test;

class GuiWidgetMethodInvokerTest {

  @Test
  void invokesOnTheLiveObjectWhenTypesMatch() throws Exception {
    GuiButtonInvokeSample source = new GuiButtonInvokeSample();
    source.seen = "live";
    Method scanned = GuiButtonInvokeSample.class.getMethod("press", Object.class);
    GuiWidgetMethodInvoker.Resolved resolved = GuiWidgetMethodInvoker.resolve(scanned, source);
    assertSame(source, resolved.target);
    GuiWidgetMethodInvoker.invoke(scanned, source);
    assertEquals("live", source.seen);
  }

  @Test
  void invokesOnTheSourceClassWhenClassLoadersDiffer() throws Exception {
    URL location = GuiButtonInvokeSample.class.getProtectionDomain().getCodeSource().getLocation();
    try (URLClassLoader otherLoader =
        new URLClassLoader(new URL[] {location}, ClassLoader.getPlatformClassLoader())) {
      Class<?> otherClass = otherLoader.loadClass(GuiButtonInvokeSample.class.getName());
      assertNotSame(GuiButtonInvokeSample.class, otherClass);

      Object source = otherClass.getDeclaredConstructor().newInstance();
      otherClass.getField("seen").set(source, "from-other");

      Method scanned = GuiButtonInvokeSample.class.getMethod("press", Object.class);
      GuiWidgetMethodInvoker.Resolved resolved = GuiWidgetMethodInvoker.resolve(scanned, source);
      assertSame(source, resolved.target);
      assertEquals(otherClass, resolved.method.getDeclaringClass());

      GuiWidgetMethodInvoker.invoke(scanned, source);
      assertEquals("from-other", otherClass.getField("seen").get(source));
    }
  }

  @Test
  void fallsBackToADummyWhenTheSourceIsADifferentClass() throws Exception {
    Method scanned = GuiButtonInvokeSample.class.getMethod("press", Object.class);
    Object unrelated = new Object();
    GuiWidgetMethodInvoker.Resolved resolved = GuiWidgetMethodInvoker.resolve(scanned, unrelated);
    assertNotSame(unrelated, resolved.target);
    assertEquals(GuiButtonInvokeSample.class, resolved.target.getClass());
  }
}
