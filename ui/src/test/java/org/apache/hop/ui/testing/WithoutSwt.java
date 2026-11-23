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

package org.apache.hop.ui.testing;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

/**
 * Loads classes the way a Hop process without SWT does (hop-run, hop-server, a Spark executor): the
 * test class path, hop-ui included, without SWT and JFace.
 */
public final class WithoutSwt extends URLClassLoader {

  public WithoutSwt() throws Exception {
    super(classpath(), ClassLoader.getPlatformClassLoader());
  }

  @Override
  protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    if (name.startsWith("org.eclipse.swt.") || name.startsWith("org.eclipse.jface.")) {
      throw new ClassNotFoundException(name);
    }
    return super.loadClass(name, resolve);
  }

  private static URL[] classpath() throws Exception {
    List<URL> urls = new ArrayList<>();
    for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
      urls.add(new File(entry).toURI().toURL());
    }
    return urls.toArray(new URL[0]);
  }

  /**
   * Asserts that each class loads, initializes and reflects without SWT, as Hop's metadata
   * serializers and command line options reflect over it: every method, field and constructor of
   * the class and its superclasses. One SWT type in a signature is enough to fail that.
   */
  public static void assertReflects(Class<?>... classes) throws Exception {
    try (WithoutSwt withoutSwt = new WithoutSwt()) {
      for (Class<?> c : classes) {
        try {
          for (Class<?> k = Class.forName(c.getName(), true, withoutSwt);
              k != null && k != Object.class;
              k = k.getSuperclass()) {
            k.getDeclaredMethods();
            k.getDeclaredFields();
            k.getDeclaredConstructors();
          }
        } catch (LinkageError e) {
          fail(c.getName() + " can't be used without SWT: " + e, e);
        }
      }
    }
  }

  /** Asserts that each class can be created with its no-argument constructor without SWT. */
  public static void assertCreates(Class<?>... classes) throws Exception {
    try (WithoutSwt withoutSwt = new WithoutSwt()) {
      for (Class<?> c : classes) {
        try {
          Class.forName(c.getName(), true, withoutSwt).getConstructor().newInstance();
        } catch (java.lang.reflect.InvocationTargetException e) {
          if (e.getCause() instanceof LinkageError linkageError) {
            fail(c.getName() + " can't be created without SWT: " + linkageError, linkageError);
          }
          throw e;
        } catch (LinkageError e) {
          fail(c.getName() + " can't be created without SWT: " + e, e);
        }
      }
    }
  }
}
