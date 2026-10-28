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

package org.apache.hop.databases.redshift;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.junit.jupiter.api.Test;

/** The database must be usable as a library, without hop-ui or SWT on the classpath. */
class RedshiftDatabaseMetaHeadlessTest {

  @Test
  void databaseLoadsWithoutTheGui() throws Exception {
    try (URLClassLoader withoutGui = new ClassLoaderWithoutGui()) {
      Object database =
          Class.forName(RedshiftDatabaseMeta.class.getName(), true, withoutGui)
              .getConstructor()
              .newInstance();

      assertEquals(
          RedshiftWidgetsListener.class.getName(),
          database.getClass().getMethod("getGuiWidgetsListenerClassName").invoke(database));
    }
  }

  @Test
  void namedListenerIsCreatedForTheDatabase() throws Exception {
    IDatabase database = new RedshiftDatabaseMeta();

    Object listener =
        Class.forName(database.getGuiWidgetsListenerClassName())
            .getConstructor(IDatabase.class)
            .newInstance(database);

    assertEquals(true, listener instanceof IGuiPluginCompositeWidgetsListener);
  }

  /** Loads everything from the test classpath itself, except the GUI classes. */
  private static class ClassLoaderWithoutGui extends URLClassLoader {
    ClassLoaderWithoutGui() throws Exception {
      super(classpath(), ClassLoader.getPlatformClassLoader());
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      if (name.startsWith("org.apache.hop.ui.") || name.startsWith("org.eclipse.swt.")) {
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
  }
}
