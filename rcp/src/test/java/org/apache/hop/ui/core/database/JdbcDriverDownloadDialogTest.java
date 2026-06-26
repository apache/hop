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

package org.apache.hop.ui.core.database;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hop.core.database.DriverDownload;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swtbot.swt.finder.SWTBot;
import org.eclipse.swtbot.swt.finder.exceptions.WidgetNotFoundException;
import org.eclipse.swtbot.swt.finder.widgets.SWTBotButton;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * SWTBot coverage for {@link JdbcDriverDownloadDialog}'s license gate. The dialog lives in the
 * {@code hop-ui} module but its SWTBot test runs here in {@code rcp}: opening it warms up {@link
 * org.apache.hop.ui.core.PropsUi}, whose {@code TextSizeUtilFacade} needs the {@code rcp}
 * implementation that {@code hop-ui} cannot depend on itself.
 *
 * <p>The dialog runs its own blocking event loop in {@code open()}, so {@link
 * SwtBotTestBase#withDialog} pumps it on the UI thread while the assertions drive it from a worker
 * thread. Both tests Cancel before any download, so nothing is fetched over the network.
 *
 * <p>Tagged {@code uitest} so it is excluded from the normal headless build (it needs a display);
 * run with {@code mvn -pl rcp -Puitest test}.
 */
@Tag("uitest")
class JdbcDriverDownloadDialogTest extends SwtBotTestBase {

  private static final Class<?> PKG = JdbcDriverDownloadDialog.class;

  /**
   * A restricted (Category X) driver: Download must stay disabled until the license is accepted.
   */
  private static DriverDownload restrictedDriver() {
    return DriverDownload.builder()
        .mavenCoordinate("com.example:demo-jdbc")
        .defaultVersion("1.0.0")
        .licenseCategory("X")
        .licenseName("Demo Proprietary License")
        .licenseUrl("https://example.com/license")
        .vendor("Example")
        .vendorUrl("https://example.com")
        .build();
  }

  /** An open (Category A) driver: Download is available immediately, no acceptance checkbox. */
  private static DriverDownload openDriver() {
    return DriverDownload.builder()
        .mavenCoordinate("org.example:open-jdbc")
        .defaultVersion("2.0.0")
        .licenseCategory("A")
        .licenseName("Apache-2.0")
        .licenseUrl("https://www.apache.org/licenses/LICENSE-2.0")
        .vendor("Example")
        .vendorUrl("https://example.com")
        .build();
  }

  @Test
  void restrictedDriverGatesDownloadOnLicenseAcceptance() {
    AtomicBoolean installed = new AtomicBoolean(true);

    withDialog(
        parent ->
            installed.set(
                new JdbcDriverDownloadDialog(parent, "DEMO", "Demo DB", restrictedDriver()).open()),
        bot -> {
          SWTBot dialog = bot.shell(shellTitle()).activate().bot();

          SWTBotButton download = dialog.button(downloadLabel());
          assertFalse(
              download.isEnabled(), "Download must be disabled until the license is accepted");

          dialog.checkBox().click(); // accept the vendor license
          assertTrue(download.isEnabled(), "accepting the license must enable Download");

          dialog.button(buttonLabel("System.Button.Cancel")).click(); // no network download
        });

    assertFalse(installed.get(), "Cancel must report the driver as not installed");
  }

  @Test
  void openDriverEnablesDownloadWithoutALicenseCheckbox() {
    AtomicBoolean installed = new AtomicBoolean(true);

    withDialog(
        parent ->
            installed.set(
                new JdbcDriverDownloadDialog(parent, "OPEN", "Open DB", openDriver()).open()),
        bot -> {
          SWTBot dialog = bot.shell(shellTitle()).activate().bot();

          assertTrue(
              dialog.button(downloadLabel()).isEnabled(),
              "an open-license driver must be downloadable without accepting anything");
          assertThrows(
              WidgetNotFoundException.class,
              dialog::checkBox,
              "an open-license driver must not show a license-acceptance checkbox");

          dialog.button(buttonLabel("System.Button.Cancel")).click();
        });

    assertFalse(installed.get(), "Cancel must report the driver as not installed");
  }

  private static String shellTitle() {
    return BaseMessages.getString(PKG, "JdbcDriverDownloadDialog.Shell.Title");
  }

  private static String downloadLabel() {
    return BaseMessages.getString(PKG, "JdbcDriverDownloadDialog.Button.Download");
  }
}
