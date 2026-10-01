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

package org.apache.hop.ui.hopgui.file;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.junit.jupiter.api.Test;

class HopFileTypeBaseTest {

  @Test
  void extractExtensionFromUnixPath() {
    assertEquals("hpl", HopFileTypeBase.extractExtension("/project/pipelines/demo.hpl"));
    assertEquals("hwf", HopFileTypeBase.extractExtension("/a/b/c.HWF"));
  }

  @Test
  void extractExtensionFromWindowsPath() {
    assertEquals("json", HopFileTypeBase.extractExtension("C:\\data\\file.JSON"));
  }

  @Test
  void extractExtensionHiddenFileHasNone() {
    assertEquals("", HopFileTypeBase.extractExtension("/home/user/.gitignore"));
    assertEquals("", HopFileTypeBase.extractExtension(".profile"));
  }

  @Test
  void extractExtensionMissing() {
    assertEquals("", HopFileTypeBase.extractExtension("README"));
    assertEquals("", HopFileTypeBase.extractExtension(""));
    assertEquals("", HopFileTypeBase.extractExtension(null));
  }

  @Test
  void extractBaseName() {
    assertEquals("demo.hpl", HopFileTypeBase.extractBaseName("/project/demo.hpl"));
    assertEquals("Dockerfile", HopFileTypeBase.extractBaseName("C:\\repo\\Dockerfile"));
  }

  @Test
  void isHandledByMatchesFilterWithoutVfs() throws HopException {
    TestFileType type = new TestFileType(new String[] {"*.hpl", "*.xml"});
    assertTrue(type.isHandledBy("/tmp/pipe.hpl", false));
    assertTrue(type.isHandledBy("C:\\x\\a.XML", false));
    assertFalse(type.isHandledBy("/tmp/pipe.hwf", false));
    assertFalse(type.isHandledBy("/tmp/noext", false));
  }

  @Test
  void isHandledBySupportsCompoundFilters() throws HopException {
    TestFileType type = new TestFileType(new String[] {"*.xls;*.xlsx"});
    assertTrue(type.isHandledBy("/data/sheet.xlsx", false));
    assertTrue(type.isHandledBy("/data/legacy.xls", false));
    assertFalse(type.isHandledBy("/data/sheet.ods", false));
  }

  /** Minimal concrete type for matching tests. */
  private static final class TestFileType extends HopFileTypeBase {
    private final String[] filters;

    private TestFileType(String[] filters) {
      this.filters = filters;
    }

    @Override
    public String getName() {
      return "test";
    }

    @Override
    public String getDefaultFileExtension() {
      return ".hpl";
    }

    @Override
    public String[] getFilterExtensions() {
      return filters;
    }

    @Override
    public String[] getFilterNames() {
      return new String[] {"test"};
    }

    @Override
    public Properties getCapabilities() {
      return new Properties();
    }

    @Override
    public IHopFileTypeHandler openFile(HopGui hopGui, String filename, IVariables variables) {
      return null;
    }

    @Override
    public IHopFileTypeHandler newFile(HopGui hopGui, IVariables parentVariableSpace) {
      return null;
    }

    @Override
    public boolean supportsFile(IHasFilename metaObject) {
      return false;
    }

    @Override
    public java.util.List<IGuiContextHandler> getContextHandlers() {
      return java.util.Collections.emptyList();
    }

    @Override
    public String getFileTypeImage() {
      return null;
    }
  }
}
