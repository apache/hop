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

package org.apache.hop.spark.engines.check;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.apache.hop.ui.core.dialog.CheckResultDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.ProgressMonitorDialog;
import org.eclipse.swt.widgets.Shell;

/** Runs the checks of a run configuration with a progress bar, and lists what they found. */
public final class SparkConfigurationTestDialog {

  private static final Class<?> PKG = SparkConfigurationTestDialog.class;

  private SparkConfigurationTestDialog() {}

  /**
   * @param metadataProvider the metadata to run the test job with, null to leave it out
   */
  public static void open(
      Shell shell,
      SparkPipelineRunConfiguration configuration,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    open(shell, configuration, variables, metadataProvider, metadataProvider != null);
  }

  /**
   * @param metadataProvider the project metadata, for the connections the checks use
   * @param testJob whether to run the test job after the checks
   */
  public static void open(
      Shell shell,
      SparkPipelineRunConfiguration configuration,
      IVariables variables,
      IHopMetadataProvider metadataProvider,
      boolean testJob) {
    List<ICheckResult> results = new ArrayList<>();
    try {
      new ProgressMonitorDialog(shell)
          .run(
              true,
              monitor ->
                  results.addAll(
                      SparkConfigurationChecker.standard()
                          .check(configuration, variables, metadataProvider, monitor, testJob)));
    } catch (Exception e) {
      new ErrorDialog(
          shell, BaseMessages.getString(PKG, "SparkCheck.Dialog.Title"), e.getMessage(), e);
      return;
    }
    CheckResultDialog dialog = new CheckResultDialog(shell, results);
    dialog.setTitle(BaseMessages.getString(PKG, "SparkCheck.Dialog.Title"));
    dialog.setSourceColumnLabel(BaseMessages.getString(PKG, "SparkCheck.Dialog.Column"));
    dialog.setEditingOrigin(false);
    dialog.open();
  }
}
