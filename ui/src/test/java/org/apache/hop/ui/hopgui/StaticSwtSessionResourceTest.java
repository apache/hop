/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.hopgui;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.hop.ui.hopgui.perspective.execution.ExecutionLogPanel;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.junit.jupiter.api.Test;

/**
 * Static SWT resources are RAP-session-unique. Caching them in {@code static} fields lets one Hop
 * Web session dispose images/widgets still used by another.
 */
class StaticSwtSessionResourceTest {

  private static final Set<Class<?>> SWT_RESOURCE_TYPES =
      Set.of(
          Image.class,
          Color.class,
          Font.class,
          Control.class,
          Shell.class,
          SashForm.class,
          GC.class);

  @Test
  void uiClassesDoNotKeepSwtResourcesInStaticFields() throws Exception {
    List<String> violations = new ArrayList<>();
    Path root =
        Path.of(
            ExecutionLogPanel.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    ClassLoader classLoader = StaticSwtSessionResourceTest.class.getClassLoader();

    try (Stream<Path> files = Files.walk(root)) {
      for (Path file :
          (Iterable<Path>) files.filter(StaticSwtSessionResourceTest::isClassFile)::iterator) {
        String className =
            root.relativize(file)
                .toString()
                .replace(java.io.File.separatorChar, '.')
                .replaceAll("\\.class$", "");
        if (!className.startsWith("org.apache.hop.ui")) {
          continue;
        }
        Class<?> clazz;
        try {
          clazz = Class.forName(className, false, classLoader);
        } catch (Throwable e) {
          continue;
        }
        for (Field field : clazz.getDeclaredFields()) {
          if (!Modifier.isStatic(field.getModifiers()) || field.isSynthetic()) {
            continue;
          }
          if (isSwtResource(field.getType())) {
            violations.add(
                clazz.getName() + "." + field.getName() + " : " + field.getType().getName());
          }
        }
      }
    }

    assertTrue(
        violations.isEmpty(), "Static SWT resource fields leak RAP UISession state: " + violations);
  }

  private static boolean isSwtResource(Class<?> type) {
    for (Class<?> swtType : SWT_RESOURCE_TYPES) {
      if (swtType.isAssignableFrom(type)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isClassFile(Path path) {
    String name = path.getFileName().toString();
    return name.endsWith(".class") && !name.contains("$");
  }
}
