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

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A Hop Server enables a project on startup just like every other Hop tool, and that sets the
 * namespace. It has no user interface, and in a container no GTK either, so anything on that path
 * that touches SWT takes the whole server down with an {@link UnsatisfiedLinkError} before it
 * serves a single request.
 */
class HopNamespaceHeadlessTest {

  private String originalRuntime;

  @BeforeEach
  void rememberRuntime() {
    originalRuntime = System.getProperty(Const.HOP_PLATFORM_RUNTIME);
    // A server, hop-run or a worker: nothing sets this.
    System.clearProperty(Const.HOP_PLATFORM_RUNTIME);
  }

  @AfterEach
  void restoreRuntime() {
    if (originalRuntime == null) {
      System.clearProperty(Const.HOP_PLATFORM_RUNTIME);
    } else {
      System.setProperty(Const.HOP_PLATFORM_RUNTIME, originalRuntime);
    }
  }

  @Test
  @DisplayName("Setting and reading a namespace without a user interface loads no SWT")
  void namespaceWorksWithoutAUserInterface() throws Exception {
    // Loaded where SWT cannot be reached at all, because on a developer machine it loads happily
    // and the test would pass either way - a container without GTK is where it bites.
    try (URLClassLoader noSwt = classLoaderWithoutSwt()) {
      Class<?> namespaceClass = noSwt.loadClass(HopNamespace.class.getName());
      namespaceClass.getMethod("setNamespace", String.class).invoke(null, "headless-project");

      assertEquals("headless-project", namespaceClass.getMethod("getNamespace").invoke(null));
    }
  }

  /** The test classpath, with every SWT class refused the way a server without GTK refuses them. */
  private URLClassLoader classLoaderWithoutSwt() throws Exception {
    List<URL> classpath = new ArrayList<>();
    for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
      classpath.add(new File(entry).toURI().toURL());
    }
    return new URLClassLoader(classpath.toArray(new URL[0]), ClassLoader.getPlatformClassLoader()) {
      @Override
      protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (name.startsWith("org.eclipse.swt")) {
          throw new ClassNotFoundException(
              "SWT must not be loaded without a user interface: " + name);
        }
        return super.loadClass(name, resolve);
      }
    };
  }
}
