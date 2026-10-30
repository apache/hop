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

package org.apache.hop.databases.oracle;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;

/** Hides the Oracle connection options that do not apply, in the connection dialog. */
public class OracleWidgetsListener implements IGuiPluginCompositeWidgetsListener {

  private final OracleDatabaseMeta database;

  public OracleWidgetsListener(IDatabase database) {
    this.database = (OracleDatabaseMeta) database;
  }

  @Override
  public void widgetsCreated(GuiCompositeWidgets compositeWidgets) {
    // Nothing to do, the values aren't set yet at this point.
  }

  @Override
  public void widgetsPopulated(GuiCompositeWidgets compositeWidgets) {
    hideFieldsThatDoNotApply(compositeWidgets);
  }

  @Override
  public void widgetModified(
      GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
    hideFieldsThatDoNotApply(compositeWidgets);
  }

  @Override
  public void persistContents(GuiCompositeWidgets compositeWidgets) {
    // Not needed, the dialog reads the widgets back itself.
  }

  /**
   * Keeps the dialog down to the fields that can actually do something for the connection being
   * edited. Most Oracle connections are a plain host and port, and a wall of TLS and wallet fields
   * that will only ever stay empty makes the ones that matter harder to find.
   */
  private void hideFieldsThatDoNotApply(GuiCompositeWidgets compositeWidgets) {
    OracleConnectionType type = readConnectionType(compositeWidgets);

    // A TNS alias and a hand written descriptor say where and how to connect all by themselves, so
    // neither the address nor the TCPS checkbox has anything left to decide. Their certificates
    // are still ours to pass on, which is why the credentials below stay put.
    //
    boolean addressApplies =
        type != OracleConnectionType.TNS_ALIAS && type != OracleConnectionType.DESCRIPTOR;
    boolean tlsApplies =
        !addressApplies
            || readCheckbox(compositeWidgets, OracleDatabaseMeta.ID_USE_TCPS, database.isUseTcps());
    OracleTlsCredentialType credentials = readCredentialType(compositeWidgets);

    Set<String> hidden = new HashSet<>();
    hideUnless(
        hidden,
        addressApplies,
        OracleDatabaseMeta.ID_HOSTNAME,
        OracleDatabaseMeta.ID_PORT,
        OracleDatabaseMeta.ID_USE_TCPS);
    hideUnless(hidden, tlsApplies, OracleDatabaseMeta.ID_TLS_CREDENTIAL_TYPE);
    hideUnless(
        hidden,
        tlsApplies,
        OracleDatabaseMeta.ID_SSL_SERVER_DN_MATCH,
        OracleDatabaseMeta.ID_SSL_SERVER_CERT_DN);
    hideUnless(
        hidden,
        tlsApplies && credentials == OracleTlsCredentialType.WALLET,
        OracleDatabaseMeta.ID_WALLET_DIRECTORY,
        OracleDatabaseMeta.ID_WALLET_PASSWORD);
    hideUnless(
        hidden,
        tlsApplies && credentials == OracleTlsCredentialType.JKS,
        OracleDatabaseMeta.ID_TRUST_STORE_FILE,
        OracleDatabaseMeta.ID_TRUST_STORE_PASSWORD,
        OracleDatabaseMeta.ID_TRUST_STORE_TYPE,
        OracleDatabaseMeta.ID_KEY_STORE_FILE,
        OracleDatabaseMeta.ID_KEY_STORE_PASSWORD,
        OracleDatabaseMeta.ID_KEY_STORE_TYPE);

    compositeWidgets.setWidgetsHidden(database, hidden);
  }

  private void hideUnless(Set<String> hidden, boolean applies, String... widgetIds) {
    if (!applies) {
      hidden.addAll(List.of(widgetIds));
    }
  }

  private OracleConnectionType readConnectionType(GuiCompositeWidgets compositeWidgets) {
    Control control = compositeWidgets.getWidgetsMap().get(OracleDatabaseMeta.ID_CONNECTION_TYPE);
    if (control instanceof Combo combo) {
      try {
        return OracleConnectionType.valueOf(combo.getText());
      } catch (IllegalArgumentException e) {
        // Nothing selected yet, the metadata is the better answer.
      }
    }
    return database.getConnectionType();
  }

  /**
   * Widgets can be switched off through disabledGuiElements.xml, in which case there is nothing to
   * read the setting from and we fall back to what is on the metadata itself.
   */
  private boolean readCheckbox(
      GuiCompositeWidgets compositeWidgets, String widgetId, boolean fallback) {
    Control control = compositeWidgets.getWidgetsMap().get(widgetId);
    return control instanceof Button button ? button.getSelection() : fallback;
  }

  private OracleTlsCredentialType readCredentialType(GuiCompositeWidgets compositeWidgets) {
    Control control =
        compositeWidgets.getWidgetsMap().get(OracleDatabaseMeta.ID_TLS_CREDENTIAL_TYPE);
    if (control instanceof Combo combo) {
      try {
        return OracleTlsCredentialType.valueOf(combo.getText());
      } catch (IllegalArgumentException e) {
        // Nothing selected yet, the metadata is the better answer.
      }
    }
    return database.getTlsCredentialType();
  }
}
