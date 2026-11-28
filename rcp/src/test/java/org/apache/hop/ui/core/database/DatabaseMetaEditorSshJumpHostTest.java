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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.SshAuthenticationMethod;
import org.apache.hop.core.database.SshJumpHost;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiEnvironment;
import org.apache.hop.ui.hopgui.perspective.metadata.MetadataPerspective;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Shell;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The jump hosts in the SSH Tunnel tab of the real connection editor: a list with buttons, and the
 * fields of the selected jump host.
 */
@Tag("uitest")
class DatabaseMetaEditorSshJumpHostTest extends SwtBotTestBase {

  private final AtomicReference<DatabaseMetaEditor> editorRef = new AtomicReference<>();

  @BeforeAll
  static void createMetadataPerspective() throws Exception {
    // The connection type widgets come from the GUI registry. init() appends without checking for
    // duplicates, so only run it when no other test in this JVM has.
    if (GuiRegistry.getInstance()
            .findGuiElements(
                BaseDatabaseMeta.class.getName(), DatabaseMeta.GUI_PLUGIN_ELEMENT_PARENT_ID)
        == null) {
      HopGuiEnvironment.init();
    }
    // The editor reports changes to the metadata perspective; it only needs to exist.
    if (MetadataPerspective.getInstance() == null) {
      new MetadataPerspective();
    }
  }

  @Test
  void jumpHostsAreListedInOrderAndTheFirstIsShown() {
    withScene(
        shell -> openEditor(shell, connectionWithTwoJumpHosts()),
        bot -> {
          assertArrayEquals(
              new String[] {"1. alice@bastion1:2222", "2. bob@${BASTION2}:22"},
              onUi(() -> jumpHostList().getItems()));
          assertEquals("bastion1", onUi(() -> text("wSshTunnelJumpHostHost").getText()));
          assertEquals(
              SshAuthenticationMethod.PRIVATE_KEY.getDescription(),
              onUi(() -> combo("wSshTunnelJumpHostAuthentication").getText()));
          assertTrue(
              onUi(() -> text("wSshTunnelJumpHostPrivateKeyFile").getTextWidget().isEnabled()));
          assertFalse(onUi(() -> text("wSshTunnelJumpHostPassword").getTextWidget().isEnabled()));
          assertFalse(onUi(() -> editor().hasChanged()), "loading is not an edit");
        });
  }

  @Test
  void selectingAJumpHostShowsItWithoutChangingTheConnection() {
    withScene(
        shell -> openEditor(shell, connectionWithTwoJumpHosts()),
        bot -> {
          onUi(() -> selectJumpHost(1));

          assertEquals("${BASTION2}", onUi(() -> text("wSshTunnelJumpHostHost").getText()));
          assertEquals("bob", onUi(() -> text("wSshTunnelJumpHostUsername").getText()));
          assertEquals("secret", onUi(() -> text("wSshTunnelJumpHostPassword").getText()));
          assertTrue(onUi(() -> text("wSshTunnelJumpHostPassword").getTextWidget().isEnabled()));
          assertFalse(
              onUi(() -> text("wSshTunnelJumpHostPrivateKeyFile").getTextWidget().isEnabled()));
          assertTrue(
              onUi(
                  () ->
                      (text("wSshTunnelJumpHostPassword").getTextWidget().getStyle() & SWT.PASSWORD)
                          != 0),
              "the jump host password is masked");
          assertFalse(onUi(() -> editor().hasChanged()), "showing a jump host is not an edit");
          assertFalse(onUi(() -> button("wSshTunnelJumpHostDown").isEnabled()), "already last");
          assertTrue(onUi(() -> button("wSshTunnelJumpHostUp").isEnabled()));
        });
  }

  @Test
  void editingTheFieldsChangesTheSelectedJumpHost() {
    withScene(
        shell -> openEditor(shell, connectionWithTwoJumpHosts()),
        bot -> {
          onUi(
              () -> {
                selectJumpHost(1);
                text("wSshTunnelJumpHostHost").setText("bastion2");
                text("wSshTunnelJumpHostPort").setText("2200");
                return null;
              });

          assertEquals("2. bob@bastion2:2200", onUi(() -> jumpHostList().getItem(1)));
          assertTrue(onUi(() -> editor().hasChanged()));

          List<SshJumpHost> saved = onUi(this::savedJumpHosts);
          assertEquals(2, saved.size());
          assertEquals("bastion1", saved.get(0).getHost());
          assertEquals("bastion2", saved.get(1).getHost());
          assertEquals("2200", saved.get(1).getPort());
          assertEquals("secret", saved.get(1).getPassword());
        });
  }

  @Test
  void addMoveAndRemoveKeepTheOrder() {
    withScene(
        shell -> openEditor(shell, connectionWithTwoJumpHosts()),
        bot -> {
          onUi(
              () -> {
                click("wSshTunnelJumpHostAdd");
                text("wSshTunnelJumpHostHost").setText("bastion3");
                click("wSshTunnelJumpHostUp");
                return null;
              });
          assertEquals(
              List.of("bastion1", "bastion3", "${BASTION2}"),
              onUi(() -> savedJumpHosts().stream().map(SshJumpHost::getHost).toList()));
          assertEquals(1, onUi(() -> jumpHostList().getSelectionIndex()), "the moved host");

          onUi(
              () -> {
                selectJumpHost(0);
                click("wSshTunnelJumpHostRemove");
                return null;
              });
          assertEquals(
              List.of("bastion3", "${BASTION2}"),
              onUi(() -> savedJumpHosts().stream().map(SshJumpHost::getHost).toList()));
          assertArrayEquals(
              new String[] {"1. bastion3:22", "2. bob@${BASTION2}:22"},
              onUi(() -> jumpHostList().getItems()));
          assertTrue(onUi(() -> editor().hasChanged()));
        });
  }

  @Test
  void switchingToAPrivateKeyEnablesTheKeyFields() {
    withScene(
        shell -> openEditor(shell, connectionWithTwoJumpHosts()),
        bot -> {
          onUi(
              () -> {
                selectJumpHost(1);
                Combo authentication = combo("wSshTunnelJumpHostAuthentication");
                authentication.select(SshAuthenticationMethod.PRIVATE_KEY.getIndex());
                authentication.notifyListeners(SWT.Selection, new Event());
                return null;
              });

          assertTrue(
              onUi(() -> text("wSshTunnelJumpHostPrivateKeyFile").getTextWidget().isEnabled()));
          assertTrue(onUi(() -> text("wSshTunnelJumpHostPassphrase").getTextWidget().isEnabled()));
          assertFalse(onUi(() -> text("wSshTunnelJumpHostPassword").getTextWidget().isEnabled()));
          assertEquals(
              SshAuthenticationMethod.PRIVATE_KEY,
              onUi(() -> savedJumpHosts().get(1).getAuthenticationMethod()));
        });
  }

  @Test
  void aDisabledTunnelDisablesTheJumpHosts() {
    withScene(
        shell -> {
          DatabaseMeta databaseMeta = connectionWithTwoJumpHosts();
          databaseMeta.setSshTunnelEnabled(false);
          openEditor(shell, databaseMeta);
        },
        bot -> {
          assertFalse(onUi(() -> jumpHostList().isEnabled()));
          assertFalse(onUi(() -> button("wSshTunnelJumpHostAdd").isEnabled()));
          assertFalse(onUi(() -> text("wSshTunnelJumpHostHost").getTextWidget().isEnabled()));
        });
  }

  // ---------------------------------------------------------------- the scene

  private static DatabaseMeta connectionWithTwoJumpHosts() {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setName("behind-bastions");
    databaseMeta.setSshTunnelEnabled(true);
    databaseMeta.setSshTunnelHost("ssh.internal");
    databaseMeta.setSshTunnelPort("22");
    databaseMeta.setSshTunnelUsername("hop");
    databaseMeta
        .getSshTunnelJumpHosts()
        .add(
            new SshJumpHost(
                "bastion1",
                "2222",
                "alice",
                SshAuthenticationMethod.PRIVATE_KEY,
                null,
                "/keys/bastion1",
                null));
    databaseMeta
        .getSshTunnelJumpHosts()
        .add(
            new SshJumpHost(
                "${BASTION2}",
                "22",
                "bob",
                SshAuthenticationMethod.PASSWORD,
                "secret",
                null,
                null));
    return databaseMeta;
  }

  /** The real editor on a bare shell, the way the metadata perspective puts it in a tab. */
  private void openEditor(Shell shell, DatabaseMeta databaseMeta) {
    shell.setLayout(new FillLayout());
    shell.setSize(900, 1000);

    HopGui hopGui = new TestHopGui();
    hopGui.getShell().dispose();
    hopGui.setShell(shell);
    hopGui.setProps(PropsUi.getInstance());

    MetadataManager<DatabaseMeta> manager =
        new MetadataManager<>(
            Variables.getADefaultVariableSpace(),
            new MemoryMetadataProvider(),
            DatabaseMeta.class,
            shell);
    DatabaseMetaEditor editor = new DatabaseMetaEditor(hopGui, manager, databaseMeta);
    Composite parent = new Composite(shell, SWT.NONE);
    parent.setLayout(new FormLayout());
    editor.createControl(parent);
    editorRef.set(editor);
    shell.layout(true, true);
  }

  private DatabaseMetaEditor editor() {
    return editorRef.get();
  }

  private List<SshJumpHost> savedJumpHosts() {
    DatabaseMeta saved = new DatabaseMeta();
    editor().getWidgetsContent(saved);
    return saved.getSshTunnelJumpHosts();
  }

  private org.eclipse.swt.widgets.List jumpHostList() {
    return widget("wSshTunnelJumpHostList");
  }

  private TextVar text(String name) {
    return widget(name);
  }

  private Combo combo(String name) {
    return widget(name);
  }

  private Button button(String name) {
    return widget(name);
  }

  private boolean selectJumpHost(int index) {
    org.eclipse.swt.widgets.List list = jumpHostList();
    list.select(index);
    list.notifyListeners(SWT.Selection, new Event());
    return true;
  }

  private void click(String name) {
    button(name).notifyListeners(SWT.Selection, new Event());
  }

  @SuppressWarnings("unchecked")
  private <T extends Control> T widget(String name) {
    try {
      Field field = DatabaseMetaEditor.class.getDeclaredField(name);
      field.setAccessible(true);
      return (T) field.get(editor());
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("No widget " + name + " in the editor", e);
    }
  }

  private static class TestHopGui extends HopGui {
    // the real Hop GUI, minus the application window it would open
  }

  /** Runs {@code supplier} on the UI thread and hands its result back to the SWTBot worker. */
  private static <T> T onUi(Supplier<T> supplier) {
    AtomicReference<T> result = new AtomicReference<>();
    AtomicReference<RuntimeException> failure = new AtomicReference<>();
    display.syncExec(
        () -> {
          try {
            result.set(supplier.get());
          } catch (RuntimeException e) {
            failure.set(e);
          }
        });
    if (failure.get() != null) {
      throw failure.get();
    }
    return result.get();
  }
}
