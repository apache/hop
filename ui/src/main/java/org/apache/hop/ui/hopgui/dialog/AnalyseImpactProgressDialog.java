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

package org.apache.hop.ui.hopgui.dialog;

import org.apache.hop.core.ProgressMonitorAdapter;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.DatabaseImpact;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.dialogs.ProgressMonitorDialog;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.swt.widgets.Shell;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * Takes care of displaying a dialog that will handle the wait while where determining the impact of a pipeline on
 * the used databases.
 *
 * @author Matt
 * @since 04-apr-2005
 */
public class AnalyseImpactProgressDialog {
  private static final Class<?> PKG = AnalyseImpactProgressDialog.class; // For Translator

  private Shell shell;
  private final IVariables variables;
  private PipelineMeta pipelineMeta;
  private List<DatabaseImpact> impact;
  private boolean impactHasRun;

  /**
   * Creates a new dialog that will handle the wait while determining the impact of the pipeline on the databases
   * used...
   */
  public AnalyseImpactProgressDialog( Shell shell, IVariables variables, PipelineMeta pipelineMeta, List<DatabaseImpact> impact ) {
    this.shell = shell;
    this.variables = variables;
    this.pipelineMeta = pipelineMeta;
    this.impact = impact;
  }

  public boolean open() {
    IRunnableWithProgress op = monitor -> {
      try {
        impact.clear(); // Start with a clean slate!!
        pipelineMeta.analyseImpact( variables, impact, new ProgressMonitorAdapter( monitor ) );
        impactHasRun = true;
      } catch ( Exception e ) {
        impact.clear();
        impactHasRun = false;
        // Problem encountered generating impact list: {0}
        throw new InvocationTargetException( e, BaseMessages.getString(
          PKG, "AnalyseImpactProgressDialog.RuntimeError.UnableToAnalyzeImpact.Exception", e.toString() ) );
      }
    };

    try {
      ProgressMonitorDialog pmd = new ProgressMonitorDialog( shell );
      pmd.run( true, true, op );
    } catch ( InvocationTargetException e ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "AnalyseImpactProgressDialog.Dialog.UnableToAnalyzeImpact.Title" ),
        BaseMessages.getString( PKG, "AnalyseImpactProgressDialog.Dialog.UnableToAnalyzeImpact.Messages" ), e );
    } catch ( InterruptedException e ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "AnalyseImpactProgressDialog.Dialog.UnableToAnalyzeImpact.Title" ),
        BaseMessages.getString( PKG, "AnalyseImpactProgressDialog.Dialog.UnableToAnalyzeImpact.Messages" ), e );
    }

    return impactHasRun;
  }
}
