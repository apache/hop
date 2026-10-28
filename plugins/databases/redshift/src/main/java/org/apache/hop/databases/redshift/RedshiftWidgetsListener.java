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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;

/** Hides the Redshift connection options that do not apply, in the connection dialog. */
public class RedshiftWidgetsListener implements IGuiPluginCompositeWidgetsListener {

  private final RedshiftDatabaseMeta database;

  public RedshiftWidgetsListener(IDatabase database) {
    this.database = (RedshiftDatabaseMeta) database;
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
   * edited. Between two deployments and four ways to authenticate, showing everything at once would
   * leave most of the tab permanently empty and the fields that matter hard to pick out.
   */
  private void hideFieldsThatDoNotApply(GuiCompositeWidgets compositeWidgets) {
    RedshiftDeploymentType deployment = readDeploymentType(compositeWidgets);
    RedshiftAuthenticationType authentication = readAuthenticationType(compositeWidgets);
    boolean serverless = deployment == RedshiftDeploymentType.SERVERLESS;

    Set<String> hidden = new HashSet<>();
    // The host name stays on offer for a serverless workgroup: left empty Hop builds the endpoint,
    // filled in it wins, which is how a custom domain name or a load balancer is reached.
    hideUnless(
        hidden, serverless, RedshiftDatabaseMeta.ID_WORKGROUP, RedshiftDatabaseMeta.ID_ACCOUNT_ID);
    // A cluster only has to name itself when IAM has to find it.
    hideUnless(hidden, !serverless && authentication.isIam(), RedshiftDatabaseMeta.ID_CLUSTER_ID);
    // The region builds the host name, and IAM needs it to know which AWS to ask.
    hideUnless(hidden, serverless || authentication.isIam(), RedshiftDatabaseMeta.ID_AWS_REGION);

    hideUnless(
        hidden,
        authentication == RedshiftAuthenticationType.IAM_CREDENTIALS,
        RedshiftDatabaseMeta.ID_AWS_ACCESS_KEY_ID,
        RedshiftDatabaseMeta.ID_AWS_SECRET_ACCESS_KEY,
        RedshiftDatabaseMeta.ID_AWS_SESSION_TOKEN);
    hideUnless(
        hidden,
        authentication == RedshiftAuthenticationType.IAM_PROFILE,
        RedshiftDatabaseMeta.ID_AWS_PROFILE);
    // Only a provisioned cluster maps IAM onto a named database user.
    hideUnless(
        hidden,
        authentication.isIam() && !serverless,
        RedshiftDatabaseMeta.ID_DB_USER,
        RedshiftDatabaseMeta.ID_DB_GROUPS,
        RedshiftDatabaseMeta.ID_AUTO_CREATE);

    compositeWidgets.setWidgetsHidden(database, hidden);
  }

  private void hideUnless(Set<String> hidden, boolean applies, String... widgetIds) {
    if (!applies) {
      hidden.addAll(List.of(widgetIds));
    }
  }

  private RedshiftDeploymentType readDeploymentType(GuiCompositeWidgets compositeWidgets) {
    Control control = compositeWidgets.getWidgetsMap().get(RedshiftDatabaseMeta.ID_DEPLOYMENT_TYPE);
    if (control instanceof Combo combo) {
      try {
        return RedshiftDeploymentType.valueOf(combo.getText());
      } catch (IllegalArgumentException e) {
        // Nothing selected yet, the metadata is the better answer.
      }
    }
    return database.getDeploymentType();
  }

  private RedshiftAuthenticationType readAuthenticationType(GuiCompositeWidgets compositeWidgets) {
    Control control =
        compositeWidgets.getWidgetsMap().get(RedshiftDatabaseMeta.ID_AUTHENTICATION_TYPE);
    if (control instanceof Combo combo) {
      try {
        return RedshiftAuthenticationType.valueOf(combo.getText());
      } catch (IllegalArgumentException e) {
        // Nothing selected yet, the metadata is the better answer.
      }
    }
    return database.getAuthenticationType();
  }
}
