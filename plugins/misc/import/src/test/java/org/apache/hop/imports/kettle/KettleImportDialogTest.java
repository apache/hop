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

package org.apache.hop.imports.kettle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.security.Permission;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.imports.gui.HopImportGuiPlugin;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.security.HopSecurityUi;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.widgets.Button;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

class KettleImportDialogTest {

  @BeforeAll
  static void initializeLogging() {
    HopLogStore.init();
  }

  @Test
  void browserImportEntryPointIsPubliclyAvailable() throws Exception {
    assertNotNull(HopImportGuiPlugin.class.getMethod("menuToolsImport", String.class));
  }

  @Test
  void configuredBrowserUploadFolderOverridesLastUsedFolder() {
    assertEquals(
        "uploaded-project",
        KettleImportDialog.initialSourceFolder("uploaded-project", "previous-project"));
  }

  @Test
  void fallsBackToLastUsedFolderForTheTraditionalMenu() {
    assertEquals(
        "previous-project", KettleImportDialog.initialSourceFolder(null, "previous-project"));
    assertEquals("", KettleImportDialog.initialSourceFolder(null, null));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void openingImportPreservesDesktopDetailsAndHidesWebServerPaths(boolean web) {
    HopGui hopGui = mock(HopGui.class);
    ILogChannel log = mock(ILogChannel.class);
    EnvironmentUtils environment = mock(EnvironmentUtils.class);
    RuntimeException failure = importFailure();
    when(hopGui.getVariables()).thenThrow(failure);
    when(hopGui.getLog()).thenReturn(log);
    when(environment.isWeb()).thenReturn(web);
    AtomicReference<List<?>> dialogArguments = new AtomicReference<>();

    try (MockedStatic<HopGui> hopGuiStatic = mockStatic(HopGui.class);
        MockedStatic<HopSecurityUi> security = mockStatic(HopSecurityUi.class);
        MockedStatic<EnvironmentUtils> environmentStatic = mockStatic(EnvironmentUtils.class);
        MockedConstruction<ErrorDialog> dialogs =
            mockConstruction(
                ErrorDialog.class, (dialog, context) -> dialogArguments.set(context.arguments()))) {
      hopGuiStatic.when(HopGui::getInstance).thenReturn(hopGui);
      security.when(() -> HopSecurityUi.check(Permission.FILE_CREATE)).thenReturn(true);
      security.when(() -> HopSecurityUi.check(Permission.METADATA_WRITE)).thenReturn(true);
      environmentStatic.when(EnvironmentUtils::getInstance).thenReturn(environment);

      new HopImportGuiPlugin().menuToolsImport("uploaded-project");

      assertEquals(1, dialogs.constructed().size());
      assertImportError(
          web, failure, dialogArguments.get(), HopImportGuiPlugin.class, "HopGuiImport");
      verify(log).logError(anyString(), same(failure));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void importingPreservesDesktopDetailsAndHidesWebServerPaths(boolean web) throws Exception {
    // Exercise the import failure path without opening an SWT display or native dialog.
    KettleImportDialog importDialog = mock(KettleImportDialog.class);
    Button importInExisting = mock(Button.class);
    EnvironmentUtils environment = mock(EnvironmentUtils.class);
    RuntimeException failure = importFailure();
    when(importInExisting.getSelection()).thenThrow(failure);
    when(environment.isWeb()).thenReturn(web);
    Field importInExistingField = KettleImportDialog.class.getDeclaredField("wImportInExisting");
    importInExistingField.setAccessible(true);
    importInExistingField.set(importDialog, importInExisting);
    Method doImport = KettleImportDialog.class.getDeclaredMethod("doImport");
    doImport.setAccessible(true);
    AtomicReference<List<?>> dialogArguments = new AtomicReference<>();

    try (MockedStatic<EnvironmentUtils> environmentStatic = mockStatic(EnvironmentUtils.class);
        MockedConstruction<ErrorDialog> dialogs =
            mockConstruction(
                ErrorDialog.class, (dialog, context) -> dialogArguments.set(context.arguments()))) {
      environmentStatic.when(EnvironmentUtils::getInstance).thenReturn(environment);

      doImport.invoke(importDialog);

      assertEquals(1, dialogs.constructed().size());
      assertImportError(
          web, failure, dialogArguments.get(), KettleImportDialog.class, "KettleImportDialog");
    }
  }

  private static RuntimeException importFailure() {
    return new IllegalStateException(
        "Unable to read /srv/private/import/kettle.properties",
        new IOException("/srv/private/config/passwords.properties"));
  }

  private static void assertImportError(
      boolean web, Exception failure, List<?> arguments, Class<?> pkg, String messagePrefix) {
    String message = (String) arguments.get(2);
    Exception displayedFailure = (Exception) arguments.get(3);
    assertFalse(message.contains("/srv/private"));
    if (web) {
      assertEquals(BaseMessages.getString(pkg, messagePrefix + ".Error.Web.Message"), message);
      assertEquals(message, displayedFailure.getMessage().trim());
      assertNull(displayedFailure.getCause());
      assertEquals(0, displayedFailure.getSuppressed().length);
    } else {
      assertEquals(BaseMessages.getString(pkg, messagePrefix + ".Error.Message"), message);
      assertFalse(message.contains("server log"));
      assertSame(failure, displayedFailure);
      assertSame(failure.getCause(), displayedFailure.getCause());
    }
  }
}
