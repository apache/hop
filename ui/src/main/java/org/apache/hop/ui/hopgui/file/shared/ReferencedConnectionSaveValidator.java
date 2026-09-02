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
package org.apache.hop.ui.hopgui.file.shared;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.validation.ReferencedDatabaseConnectionChecker;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.dialog.MessageDialogWithToggle;
import org.apache.hop.ui.hopgui.file.config.FileValidationConfigPlugin;
import org.apache.hop.workflow.WorkflowMeta;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Shell;

/**
 * Optional warning when saving a pipeline or workflow that references a missing database
 * connection. Saving is never blocked: the user can continue or cancel.
 */
public final class ReferencedConnectionSaveValidator {

  private static final Class<?> PKG = FileValidationConfigPlugin.class;
  private static final int MAX_LISTED = 15;

  private ReferencedConnectionSaveValidator() {}

  public static boolean confirmSave(
      Shell shell,
      PipelineMeta pipelineMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    if (!FileValidationConfigPlugin.getInstance().isValidateDbConnectionsOnSave()) {
      return true;
    }
    return confirmRemarks(
        shell,
        ReferencedDatabaseConnectionChecker.checkPipeline(
            pipelineMeta, variables, metadataProvider));
  }

  public static boolean confirmSave(
      Shell shell,
      WorkflowMeta workflowMeta,
      IVariables variables,
      IHopMetadataProvider metadataProvider) {
    if (!FileValidationConfigPlugin.getInstance().isValidateDbConnectionsOnSave()) {
      return true;
    }
    return confirmRemarks(
        shell,
        ReferencedDatabaseConnectionChecker.checkWorkflow(
            workflowMeta, variables, metadataProvider));
  }

  static boolean confirmRemarks(Shell shell, List<ICheckResult> remarks) {
    if (remarks == null || remarks.isEmpty()) {
      return true;
    }
    if (shell == null || shell.isDisposed()) {
      return true;
    }

    String listed =
        remarks.stream()
            .limit(MAX_LISTED)
            .map(ICheckResult::getText)
            .collect(Collectors.joining("\n"));
    if (remarks.size() > MAX_LISTED) {
      listed =
          listed
              + "\n"
              + BaseMessages.getString(
                  PKG,
                  "ReferencedConnectionSaveValidator.Dialog.More",
                  Integer.toString(remarks.size() - MAX_LISTED));
    }

    MessageDialogWithToggle dialog =
        new MessageDialogWithToggle(
            shell,
            BaseMessages.getString(PKG, "ReferencedConnectionSaveValidator.Dialog.Title"),
            BaseMessages.getString(PKG, "ReferencedConnectionSaveValidator.Dialog.Message", listed),
            SWT.ICON_WARNING,
            new String[] {
              BaseMessages.getString(PKG, "System.Button.Yes"),
              BaseMessages.getString(PKG, "System.Button.No")
            },
            BaseMessages.getString(PKG, "ReferencedConnectionSaveValidator.Dialog.Toggle"),
            false);
    int answer = dialog.open();

    if (dialog.getToggleState()) {
      FileValidationConfigPlugin config = FileValidationConfigPlugin.getInstance();
      config.setValidateDbConnectionsOnSave(false);
      config.saveToHopConfig();
    }

    return answer == 0;
  }
}
