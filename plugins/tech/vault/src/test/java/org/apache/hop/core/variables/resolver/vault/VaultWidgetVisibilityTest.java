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

package org.apache.hop.core.variables.resolver.vault;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.variables.resolver.VariableResolver;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGuiEnvironment;
import org.apache.hop.ui.testing.SwtBotTestBase;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * TOKEN and KUBERNETES have no credentials in common, so the editor only shows the ones the chosen
 * type can actually use.
 */
@Tag("uitest")
class VaultWidgetVisibilityTest extends SwtBotTestBase {

  @BeforeAll
  static void registerGuiPluginElements() throws Exception {
    if (GuiRegistry.getInstance()
            .findGuiElements(
                VaultVariableResolver.class.getName(),
                VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID)
        == null) {
      HopGuiEnvironment.init();
    }
  }

  @Test
  @DisplayName("TOKEN shows the Vault token and hides Kubernetes fields")
  void tokenShowsOnlyTheToken() {
    withWidgets(
        resolver -> resolver.setAuthenticationType(VaultAuthType.TOKEN.name()),
        (resolver, widgets) -> {
          assertVisible(widgets, BaseVaultVariableResolver.ID_AUTHENTICATION_TYPE);
          assertVisible(widgets, BaseVaultVariableResolver.ID_VAULT_TOKEN);

          assertHidden(widgets, BaseVaultVariableResolver.ID_KUBERNETES_ROLE);
          assertHidden(widgets, BaseVaultVariableResolver.ID_KUBERNETES_JWT_PATH);
          assertHidden(widgets, BaseVaultVariableResolver.ID_KUBERNETES_JWT);
          assertHidden(widgets, BaseVaultVariableResolver.ID_KUBERNETES_AUTH_PATH);
        });
  }

  @Test
  @DisplayName("KUBERNETES shows the Kubernetes fields and hides the Vault token")
  void kubernetesShowsOnlyKubernetesFields() {
    withWidgets(
        resolver -> resolver.setAuthenticationType(VaultAuthType.KUBERNETES.name()),
        (resolver, widgets) -> {
          assertVisible(widgets, BaseVaultVariableResolver.ID_AUTHENTICATION_TYPE);
          assertVisible(widgets, BaseVaultVariableResolver.ID_KUBERNETES_ROLE);
          assertVisible(widgets, BaseVaultVariableResolver.ID_KUBERNETES_JWT_PATH);
          assertVisible(widgets, BaseVaultVariableResolver.ID_KUBERNETES_JWT);
          assertVisible(widgets, BaseVaultVariableResolver.ID_KUBERNETES_AUTH_PATH);

          assertHidden(widgets, BaseVaultVariableResolver.ID_VAULT_TOKEN);
        });
  }

  @Test
  @DisplayName("switching to KUBERNETES clears a leftover Vault token from the hidden widget")
  void kubernetesClearsLeftoverVaultToken() {
    withWidgets(
        resolver -> {
          resolver.setAuthenticationType(VaultAuthType.KUBERNETES.name());
          resolver.setVaultToken("s.leftover");
        },
        (resolver, widgets) -> {
          resolver.persistContents(widgets);
          assertEquals("", resolver.getVaultToken());
          assertEquals("", textOf(widgets, BaseVaultVariableResolver.ID_VAULT_TOKEN));
        });
  }

  @Test
  @DisplayName("a variable auth type keeps every credential field visible and intact")
  void variableAuthTypeShowsAllCredentialFields() {
    withWidgets(
        resolver -> {
          resolver.setAuthenticationType("${VAULT_AUTH_TYPE}");
          resolver.setVaultToken("s.token");
          resolver.setKubernetesJwt("inline-jwt");
          resolver.setKubernetesRole("hop");
        },
        (resolver, widgets) -> {
          assertVisible(widgets, BaseVaultVariableResolver.ID_VAULT_TOKEN);
          assertVisible(widgets, BaseVaultVariableResolver.ID_KUBERNETES_ROLE);
          assertVisible(widgets, BaseVaultVariableResolver.ID_KUBERNETES_JWT_PATH);
          assertVisible(widgets, BaseVaultVariableResolver.ID_KUBERNETES_JWT);
          assertVisible(widgets, BaseVaultVariableResolver.ID_KUBERNETES_AUTH_PATH);

          resolver.persistContents(widgets);
          assertEquals("s.token", resolver.getVaultToken());
          assertEquals("inline-jwt", resolver.getKubernetesJwt());
          assertEquals("hop", resolver.getKubernetesRole());
        });
  }

  @Test
  @DisplayName("connection and secret options stay visible for every auth type")
  void generalOptionsStayVisibleForEveryAuthType() {
    for (VaultAuthType authType : VaultAuthType.values()) {
      withWidgets(
          resolver -> resolver.setAuthenticationType(authType.name()),
          (resolver, widgets) -> {
            assertVisible(widgets, BaseVaultVariableResolver.ID_VAULT_ADDRESS);
            assertVisible(widgets, BaseVaultVariableResolver.ID_NAMESPACE);
            assertVisible(widgets, BaseVaultVariableResolver.ID_VERIFYING_SSL);
            assertVisible(widgets, BaseVaultVariableResolver.ID_PEM_FILE_PATH);
            assertVisible(widgets, BaseVaultVariableResolver.ID_PEM_STRING);
            assertVisible(widgets, BaseVaultVariableResolver.ID_OPEN_TIMEOUT);
            assertVisible(widgets, BaseVaultVariableResolver.ID_READ_TIMEOUT);
            assertVisible(widgets, BaseVaultVariableResolver.ID_PATH_PREFIX);
            assertVisible(widgets, BaseVaultVariableResolver.ID_AUTHENTICATION_TYPE);
          });
    }
  }

  private void withWidgets(
      Consumer<VaultVariableResolver> configure,
      BiConsumer<VaultVariableResolver, GuiCompositeWidgets> assertions) {
    ensureDisplay();

    Shell shell = new Shell(display);
    shell.setLayout(new FormLayout());
    try {
      VaultVariableResolver resolver = new VaultVariableResolver();
      configure.accept(resolver);

      GuiCompositeWidgets widgets = new GuiCompositeWidgets(new Variables());
      widgets.createCompositeWidgets(
          resolver, null, shell, VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID, null);
      widgets.setWidgetsContents(resolver, shell, VariableResolver.GUI_PLUGIN_ELEMENT_PARENT_ID);
      widgets.setWidgetsListener(resolver);
      resolver.widgetsPopulated(widgets);

      shell.layout(true, true);
      shell.pack();

      assertions.accept(resolver, widgets);
    } finally {
      if (!shell.isDisposed()) {
        shell.dispose();
      }
    }
  }

  private void assertVisible(GuiCompositeWidgets widgets, String id) {
    Control control = widgets.getWidgetsMap().get(id);
    assertNotNull(control, "no widget registered for " + id);
    assertTrue(control.getVisible(), id + " should be visible");
    Control label = widgets.getLabelsMap().get(id);
    if (label != null) {
      assertTrue(label.getVisible(), "the label of " + id + " should be visible");
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
  }

  private static String textOf(GuiCompositeWidgets widgets, String id) {
    Control control = widgets.getWidgetsMap().get(id);
    if (control instanceof TextVar textVar) {
      return textVar.getText();
    }
    if (control instanceof Text text) {
      return text.getText();
    }
    return null;
  }
}
