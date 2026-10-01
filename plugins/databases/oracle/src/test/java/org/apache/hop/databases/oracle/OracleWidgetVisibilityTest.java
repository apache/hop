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

package org.apache.hop.databases.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.hopgui.HopGuiEnvironment;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The Oracle connection has a lot of options that only apply to some connections. This checks that
 * the ones that cannot do anything are actually out of the way, and -- just as important -- that
 * the ones that do apply are still there.
 */
@Tag("uitest")
class OracleWidgetVisibilityTest extends SwtBotTestBase {

  @BeforeAll
  static void registerGuiPluginElements() throws Exception {
    // Puts the @GuiWidgetElement annotations of this plugin into the registry. Without it the
    // composite comes up empty, and only by accident of test ordering would another class have
    // done it for us.
    //
    // The registry appends without checking for duplicates and init() re-scans everything, so
    // calling it a second time in the same JVM would register every element twice. Another test
    // class in this module initialises it too, hence the check.
    //
    if (GuiRegistry.getInstance()
            .findGuiElements(
                OracleDatabaseMeta.class.getName(), DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID)
        == null) {
      HopGuiEnvironment.init();
    }
  }

  @Test
  @DisplayName("a plain connection shows no TLS fields at all")
  void plainConnectionHidesTls() {
    withWidgets(
        meta -> {
          // Nothing configured: the default a new Oracle connection starts life with.
        },
        (meta, widgets) -> {
          assertVisible(widgets, OracleDatabaseMeta.ID_HOSTNAME);
          assertVisible(widgets, OracleDatabaseMeta.ID_PORT);
          assertVisible(widgets, OracleDatabaseMeta.ID_CONNECTION_TYPE);
          assertVisible(widgets, OracleDatabaseMeta.ID_TNS_ADMIN);
          assertVisible(widgets, OracleDatabaseMeta.ID_USE_TCPS);

          assertHidden(widgets, OracleDatabaseMeta.ID_TLS_CREDENTIAL_TYPE);
          assertHidden(widgets, OracleDatabaseMeta.ID_SSL_SERVER_DN_MATCH);
          assertHidden(widgets, OracleDatabaseMeta.ID_SSL_SERVER_CERT_DN);
          assertHidden(widgets, OracleDatabaseMeta.ID_WALLET_DIRECTORY);
          assertHidden(widgets, OracleDatabaseMeta.ID_TRUST_STORE_FILE);
          assertHidden(widgets, OracleDatabaseMeta.ID_KEY_STORE_FILE);
        });
  }

  @Test
  @DisplayName("switching TLS on reveals the credential choice but not both credential sets")
  void tlsShowsCredentialChoiceOnly() {
    withWidgets(
        meta -> meta.setUseTcps(true),
        (meta, widgets) -> {
          assertVisible(widgets, OracleDatabaseMeta.ID_TLS_CREDENTIAL_TYPE);
          assertVisible(widgets, OracleDatabaseMeta.ID_SSL_SERVER_DN_MATCH);
          // Credentials default to NONE, so neither set of files applies yet.
          assertHidden(widgets, OracleDatabaseMeta.ID_WALLET_DIRECTORY);
          assertHidden(widgets, OracleDatabaseMeta.ID_TRUST_STORE_FILE);
        });
  }

  @Test
  @DisplayName("picking a wallet shows the wallet fields and only those")
  void walletHidesTheKeyStoreFields() {
    withWidgets(
        meta -> {
          meta.setUseTcps(true);
          meta.setTlsCredentialType(OracleTlsCredentialType.WALLET);
        },
        (meta, widgets) -> {
          assertVisible(widgets, OracleDatabaseMeta.ID_WALLET_DIRECTORY);
          assertVisible(widgets, OracleDatabaseMeta.ID_WALLET_PASSWORD);

          assertHidden(widgets, OracleDatabaseMeta.ID_TRUST_STORE_FILE);
          assertHidden(widgets, OracleDatabaseMeta.ID_TRUST_STORE_PASSWORD);
          assertHidden(widgets, OracleDatabaseMeta.ID_TRUST_STORE_TYPE);
          assertHidden(widgets, OracleDatabaseMeta.ID_KEY_STORE_FILE);
          assertHidden(widgets, OracleDatabaseMeta.ID_KEY_STORE_PASSWORD);
          assertHidden(widgets, OracleDatabaseMeta.ID_KEY_STORE_TYPE);
        });
  }

  @Test
  @DisplayName("picking JKS shows the keystore fields and hides the wallet")
  void jksHidesTheWalletFields() {
    withWidgets(
        meta -> {
          meta.setUseTcps(true);
          meta.setTlsCredentialType(OracleTlsCredentialType.JKS);
        },
        (meta, widgets) -> {
          assertVisible(widgets, OracleDatabaseMeta.ID_TRUST_STORE_FILE);
          assertVisible(widgets, OracleDatabaseMeta.ID_KEY_STORE_FILE);

          assertHidden(widgets, OracleDatabaseMeta.ID_WALLET_DIRECTORY);
          assertHidden(widgets, OracleDatabaseMeta.ID_WALLET_PASSWORD);
        });
  }

  @Test
  @DisplayName("a TNS alias drops the address fields but keeps the credentials reachable")
  void tnsAliasHidesTheAddressAndTcpsCheckbox() {
    withWidgets(
        meta -> meta.setConnectionType(OracleConnectionType.TNS_ALIAS),
        (meta, widgets) -> {
          // The alias resolves the address and the protocol, so none of this can do anything.
          assertHidden(widgets, OracleDatabaseMeta.ID_HOSTNAME);
          assertHidden(widgets, OracleDatabaseMeta.ID_PORT);
          assertHidden(widgets, OracleDatabaseMeta.ID_USE_TCPS);

          // But tnsnames.ora may well name protocol=tcps, and then the certificates are still
          // ours to pass on.
          assertVisible(widgets, OracleDatabaseMeta.ID_TLS_CREDENTIAL_TYPE);
          assertVisible(widgets, OracleDatabaseMeta.ID_TNS_ADMIN);
        });
  }

  @Test
  @DisplayName("a hand written descriptor behaves the same way")
  void descriptorHidesTheAddress() {
    withWidgets(
        meta -> meta.setConnectionType(OracleConnectionType.DESCRIPTOR),
        (meta, widgets) -> {
          assertHidden(widgets, OracleDatabaseMeta.ID_HOSTNAME);
          assertHidden(widgets, OracleDatabaseMeta.ID_USE_TCPS);
          assertVisible(widgets, OracleDatabaseMeta.ID_TLS_CREDENTIAL_TYPE);
        });
  }

  @Test
  @DisplayName("hiding a row closes the gap it leaves behind")
  void hiddenRowsDoNotLeaveHoles() {
    withWidgets(
        meta -> {
          meta.setUseTcps(true);
          meta.setTlsCredentialType(OracleTlsCredentialType.WALLET);
        },
        (meta, widgets) -> {
          // Wallet is on, so the six keystore rows in between (orders 10-15) are hidden. The row
          // below them has to hang off the last row that is still visible rather than off a
          // hidden one, otherwise the dialog keeps a hole the size of six rows.
          //
          assertHangsBelow(
              widgets,
              OracleDatabaseMeta.ID_SSL_SERVER_DN_MATCH,
              OracleDatabaseMeta.ID_WALLET_PASSWORD);

          // And with the keystore fields showing instead, the chain runs through them again.
          // Driven through the widget, because that is what the dialog reads.
          //
          Combo credentials =
              (Combo) widgets.getWidgetsMap().get(OracleDatabaseMeta.ID_TLS_CREDENTIAL_TYPE);
          credentials.setText(OracleTlsCredentialType.JKS.name());
          meta.widgetModified(widgets, credentials, OracleDatabaseMeta.ID_TLS_CREDENTIAL_TYPE);
          assertHangsBelow(
              widgets,
              OracleDatabaseMeta.ID_SSL_SERVER_DN_MATCH,
              OracleDatabaseMeta.ID_KEY_STORE_TYPE);
        });
  }

  @Test
  @DisplayName("hidden rows do not reserve height in the composite")
  void hiddenRowsDoNotReserveHeight() {
    // FormLayout measures invisible controls exactly like visible ones, so hiding a row is only
    // half the job: unless its height is taken away too, the dialog reserves the space anyway and
    // the gap simply moves to the bottom. It showed up as a band of white between the last Oracle
    // field and the Manual URL box below the composite.
    //
    int plain = compositeHeight(meta -> {});
    int everything =
        compositeHeight(
            meta -> {
              meta.setUseTcps(true);
              meta.setTlsCredentialType(OracleTlsCredentialType.JKS);
            });

    // A connection with twelve rows hidden cannot need more room than one showing all of them.
    assertTrue(
        plain <= everything,
        "a default connection ("
            + plain
            + "px) must not be taller than one with every TLS field showing ("
            + everything
            + "px)");
  }

  /** The height the plugin's composite asks for, which is what pushes the rest of the tab down. */
  private int compositeHeight(Consumer<OracleDatabaseMeta> configure) {
    ensureDisplay();
    Shell shell = new Shell(display);
    shell.setLayout(new FormLayout());
    try {
      OracleDatabaseMeta meta = new OracleDatabaseMeta();
      meta.setPluginId("ORACLE");
      configure.accept(meta);

      // An auto-sized composite inside the shell, the way DatabaseMetaEditor hangs the plugin
      // widgets off wDatabaseSpecificComp: no bottom attachment, so its computed height is what
      // everything below it gets pushed down by.
      //
      Composite inner = new Composite(shell, SWT.NONE);
      inner.setLayout(new FormLayout());
      FormData fdInner = new FormData();
      fdInner.left = new FormAttachment(0, 0);
      fdInner.right = new FormAttachment(100, 0);
      fdInner.top = new FormAttachment(0, 0);
      inner.setLayoutData(fdInner);

      GuiCompositeWidgets widgets = new GuiCompositeWidgets(new Variables());
      widgets.createCompositeWidgets(
          meta, null, inner, DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID, null);
      widgets.setWidgetsContents(meta, inner, DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
      widgets.setWidgetsListener(meta);
      meta.widgetsPopulated(widgets);

      shell.layout(true, true);
      shell.pack();

      return inner.computeSize(SWT.DEFAULT, SWT.DEFAULT).y;
    } finally {
      if (!shell.isDisposed()) {
        shell.dispose();
      }
    }
  }

  /**
   * A row is positioned by the top attachment of its label (or of the widget itself when it has no
   * label), so that is where the re-hanging shows up.
   */
  private void assertHangsBelow(GuiCompositeWidgets widgets, String id, String expectedAboveId) {
    Control anchor = widgets.getLabelsMap().get(id);
    if (anchor == null) {
      anchor = widgets.getWidgetsMap().get(id);
    }
    assertNotNull(anchor, "no widget registered for " + id);

    Control expectedAbove = widgets.getWidgetsMap().get(expectedAboveId);
    assertNotNull(expectedAbove, "no widget registered for " + expectedAboveId);

    FormData formData = (FormData) anchor.getLayoutData();
    assertNotNull(formData, id + " has no layout data");
    assertSame(
        expectedAbove,
        formData.top.control,
        id + " should hang below " + expectedAboveId + ", the last visible row above it");
  }

  @Test
  @DisplayName("the browse button of a hidden folder field goes with it")
  void hiddenFolderRowTakesItsBrowseButton() {
    withWidgets(
        meta -> {
          // Wallet directory is a FOLDER element, so it has a Browse button beside it.
        },
        (meta, widgets) -> {
          Control browse =
              widgets.getActionWidgetsMap().get(OracleDatabaseMeta.ID_WALLET_DIRECTORY);
          assertNotNull(browse, "the wallet directory should have a Browse button");
          assertFalse(browse.getVisible(), "a hidden row must not leave its Browse button behind");
        });
  }

  @Test
  @DisplayName("turning an option back off shows the fields again")
  void hidingIsReversible() {
    withWidgets(
        meta -> {
          meta.setUseTcps(true);
          meta.setTlsCredentialType(OracleTlsCredentialType.WALLET);
        },
        (meta, widgets) -> {
          assertVisible(widgets, OracleDatabaseMeta.ID_WALLET_DIRECTORY);

          // The same path the dialog takes when the user clicks the checkbox: the widget changes
          // and the modify callback fires. The widget is what is read, not the metadata, which is
          // only written back when the dialog is saved.
          //
          Button useTcps = (Button) widgets.getWidgetsMap().get(OracleDatabaseMeta.ID_USE_TCPS);

          useTcps.setSelection(false);
          meta.widgetModified(widgets, useTcps, OracleDatabaseMeta.ID_USE_TCPS);
          assertHidden(widgets, OracleDatabaseMeta.ID_WALLET_DIRECTORY);
          assertHidden(widgets, OracleDatabaseMeta.ID_TLS_CREDENTIAL_TYPE);

          useTcps.setSelection(true);
          meta.widgetModified(widgets, useTcps, OracleDatabaseMeta.ID_USE_TCPS);
          assertVisible(widgets, OracleDatabaseMeta.ID_WALLET_DIRECTORY);
          assertVisible(widgets, OracleDatabaseMeta.ID_TLS_CREDENTIAL_TYPE);
        });
  }

  /**
   * Builds the plugin's composite the way {@code DatabaseMetaEditor} does: create the widgets, push
   * the values in, then let the plugin react to them.
   */
  private void withWidgets(
      Consumer<OracleDatabaseMeta> configure,
      java.util.function.BiConsumer<OracleDatabaseMeta, GuiCompositeWidgets> assertions) {
    ensureDisplay();

    Shell shell = new Shell(display);
    shell.setLayout(new FormLayout());
    try {
      OracleDatabaseMeta meta = new OracleDatabaseMeta();
      meta.setPluginId("ORACLE");
      configure.accept(meta);

      GuiCompositeWidgets widgets = new GuiCompositeWidgets(new Variables());
      widgets.createCompositeWidgets(
          meta, null, shell, DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID, null);
      widgets.setWidgetsContents(meta, shell, DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
      widgets.setWidgetsListener(meta);
      meta.widgetsPopulated(widgets);

      shell.layout(true, true);
      shell.pack();

      assertions.accept(meta, widgets);
    } finally {
      if (!shell.isDisposed()) {
        shell.dispose();
      }
    }
  }

  /**
   * The widget framework catches and prints the exceptions raised while reading values back, so a
   * regression shows up as console noise rather than a failure. This is how we get at it.
   */
  private String captureStdErr(Runnable work) {
    PrintStream originalErr = System.err;
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    try {
      System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
      work.run();
    } finally {
      System.setErr(originalErr);
    }
    return captured.toString(StandardCharsets.UTF_8);
  }

  // getVisible() rather than isVisible(): the latter walks up to the shell, which is never opened
  // in a test, so it would report everything as invisible.
  //
  private void assertVisible(GuiCompositeWidgets widgets, String id) {
    Control control = widgets.getWidgetsMap().get(id);
    assertNotNull(control, "no widget registered for " + id);
    assertTrue(control.getVisible(), id + " should be visible");
    Control label = widgets.getLabelsMap().get(id);
    if (label != null) {
      assertTrue(label.getVisible(), "the label of " + id + " should be visible");
    }
    Control action = widgets.getActionWidgetsMap().get(id);
    if (action != null) {
      assertTrue(action.getVisible(), "the Browse button of " + id + " should be visible");
    }
  }

  private void assertHidden(GuiCompositeWidgets widgets, String id) {
    Control control = widgets.getWidgetsMap().get(id);
    assertNotNull(control, "no widget registered for " + id);
    assertFalse(control.getVisible(), id + " should be hidden");
    Control label = widgets.getLabelsMap().get(id);
    if (label != null) {
      assertFalse(label.getVisible(), "the label of " + id + " should be hidden");
    }
    Control action = widgets.getActionWidgetsMap().get(id);
    if (action != null) {
      assertFalse(action.getVisible(), "the Browse button of " + id + " should be hidden");
    }
  }

  @Test
  @DisplayName("a brand new connection round trips through the widgets without losing its enums")
  void newConnectionSurvivesTheEditorRoundTrip() {
    ensureDisplay();

    Shell shell = new Shell(display);
    shell.setLayout(new FormLayout());
    try {
      // Exactly what the dialog holds right after picking Oracle from the database type combo.
      //
      OracleDatabaseMeta meta = new OracleDatabaseMeta();
      meta.setPluginId("ORACLE");

      GuiCompositeWidgets widgets = new GuiCompositeWidgets(new Variables());
      widgets.createCompositeWidgets(
          meta, null, shell, DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID, null);
      widgets.setWidgetsListener(meta);

      // setWidgetsContents() calls Combo.setText(), which fires a Modify event, which sends the
      // editor straight back into getWidgetsContents() while the rest of the widgets are still
      // empty. That re-entrancy is what used to fill the console with stack traces: the framework
      // swallows the exception and prints it, so the only way to catch a regression is to watch
      // what gets printed.
      //
      String printed =
          captureStdErr(
              () -> {
                widgets.setWidgetsContents(meta, shell, DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
                widgets.getWidgetsContents(meta, DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
              });

      assertFalse(
          printed.contains("IllegalArgumentException"),
          "creating an Oracle connection should not print exceptions, got:\n" + printed);
      assertEquals(OracleConnectionType.AUTOMATIC, meta.getConnectionType());
      assertEquals(OracleTlsCredentialType.NONE, meta.getTlsCredentialType());
    } finally {
      if (!shell.isDisposed()) {
        shell.dispose();
      }
    }
  }

  @Test
  @DisplayName("a half filled combo never writes an empty value onto the connection")
  void emptyComboLeavesTheValueAlone() {
    withWidgets(
        meta -> meta.setConnectionType(OracleConnectionType.SERVICE_NAME),
        (meta, widgets) -> {
          Combo combo = (Combo) widgets.getWidgetsMap().get(OracleDatabaseMeta.ID_CONNECTION_TYPE);
          assertEquals("SERVICE_NAME", combo.getText());

          // What the widget looks like mid-populate, and what a read at that moment must not do.
          //
          combo.setText("");
          String printed =
              captureStdErr(
                  () ->
                      widgets.getWidgetsContents(meta, DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID));

          assertEquals(OracleConnectionType.SERVICE_NAME, meta.getConnectionType());
          assertFalse(
              printed.contains("IllegalArgumentException"),
              "an empty combo should be ignored, not thrown over, got:\n" + printed);
        });
  }

  @Test
  @DisplayName("picking a connection type in the combo reaches the URL the dialog tests with")
  void comboSelectionReachesTheGeneratedUrl() {
    withWidgets(
        meta -> {
          // A connection as it comes up before the user touches anything.
        },
        (meta, widgets) -> {
          Combo combo = (Combo) widgets.getWidgetsMap().get(OracleDatabaseMeta.ID_CONNECTION_TYPE);
          combo.setText(OracleConnectionType.SERVICE_NAME.name());
          meta.widgetModified(widgets, combo, OracleDatabaseMeta.ID_CONNECTION_TYPE);

          // getWidgetsContents() is what the Test button runs before it builds the URL, so this is
          // the path between the combo and what actually gets connected to.
          widgets.getWidgetsContents(meta, DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);

          assertEquals(OracleConnectionType.SERVICE_NAME, meta.getConnectionType());
          try {
            assertEquals(
                "jdbc:oracle:thin:@//oracle:1521/FREEPDB1",
                meta.getURL("oracle", "1521", "FREEPDB1"),
                "a service name must not come out as the SID form");
          } catch (Exception e) {
            throw new IllegalStateException(e);
          }
        });
  }

  @Test
  @DisplayName("a combo value applies even when the plugin class came from another classloader")
  void enumSurvivesASecondClassloader() throws Exception {
    // The registry captures the field class when it scans plugins; the object being written to
    // comes from wherever DatabaseMeta.setDatabaseType() loaded the plugin. In a running Hop those
    // are two different Class objects with the same name, and an enum constant resolved against
    // the registry's copy is rejected by reflection as an "argument type mismatch". Resolving
    // against the setter's own parameter type is what makes this work.
    //
    withWidgets(
        meta -> {
          // Nothing configured; the combo drives it below.
        },
        (meta, widgets) -> {
          Combo combo = (Combo) widgets.getWidgetsMap().get(OracleDatabaseMeta.ID_CONNECTION_TYPE);
          combo.setText(OracleConnectionType.SERVICE_NAME.name());

          // A separate instance, the way DatabaseMetaEditor.test() builds one.
          DatabaseMeta fresh = new DatabaseMeta();
          fresh.setDatabaseType("Oracle");
          widgets.getWidgetsContents(
              fresh.getIDatabase(), DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
          fresh.setAccessType(DatabaseMeta.TYPE_ACCESS_NATIVE);

          OracleDatabaseMeta target = (OracleDatabaseMeta) fresh.getIDatabase();
          assertEquals(OracleConnectionType.SERVICE_NAME, target.getConnectionType());
          try {
            assertEquals(
                "jdbc:oracle:thin:@//oracle:1521/FREEPDB1",
                target.getURL("oracle", "1521", "FREEPDB1"));
          } catch (Exception e) {
            throw new IllegalStateException(e);
          }
        });
  }

  @Test
  @DisplayName("the label does not collide with the database type selector above it")
  void connectionTypeLabelIsDistinct() {
    // Both the database type combo of the editor and this one used to read "Connection type".
    assertEquals(
        "Connect using",
        org.apache.hop.i18n.BaseMessages.getString(
            OracleDatabaseMeta.class, "OracleDatabaseMeta.label.ConnectionType"));
  }
}
