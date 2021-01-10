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
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.operation.IRunnableWithProgress;

import java.lang.reflect.InvocationTargetException;

/**
 * @author Matt
 * @since 10-mrt-2005
 */
public class SearchFieldsProgressDialog implements IRunnableWithProgress {
  private static final Class<?> PKG = SearchFieldsProgressDialog.class; // For Translator

  private final IVariables variables;
  private TransformMeta transformMeta;
  private boolean before;
  private PipelineMeta pipelineMeta;
  private IRowMeta fields;

  public SearchFieldsProgressDialog( IVariables variables, PipelineMeta pipelineMeta, TransformMeta transformMeta, boolean before ) {
    this.variables = variables;
    this.pipelineMeta = pipelineMeta;
    this.transformMeta = transformMeta;
    this.before = before;
    this.fields = null;
  }

  public void run( IProgressMonitor monitor ) throws InvocationTargetException, InterruptedException {
    int size = pipelineMeta.findNrPrevTransforms( transformMeta );

    try {
      if ( before ) {
        monitor.beginTask( BaseMessages.getString(
          PKG, "SearchFieldsProgressDialog.Dialog.SearchInputFields.Message" ), size ); // Searching
        // for
        // input
        // fields...
        fields = pipelineMeta.getPrevTransformFields( variables, transformMeta, new ProgressMonitorAdapter( monitor ) );
      } else {
        monitor.beginTask( BaseMessages.getString(
          PKG, "SearchFieldsProgressDialog.Dialog.SearchOutputFields.Message" ), size ); // Searching
        // for
        // output
        // fields...
        fields = pipelineMeta.getTransformFields( variables, transformMeta, new ProgressMonitorAdapter( monitor ) );
      }
    } catch ( HopTransformException kse ) {
      throw new InvocationTargetException( kse, BaseMessages.getString(
        PKG, "SearchFieldsProgressDialog.Log.UnableToGetFields", transformMeta.toString(), kse.getMessage() ) );
    }

    monitor.done();
  }

  /**
   * @return Returns the before.
   */
  public boolean isBefore() {
    return before;
  }

  /**
   * @param before The before to set.
   */
  public void setBefore( boolean before ) {
    this.before = before;
  }

  /**
   * @return Returns the fields.
   */
  public IRowMeta getFields() {
    return fields;
  }

  /**
   * @param fields The fields to set.
   */
  public void setFields( IRowMeta fields ) {
    this.fields = fields;
  }

  /**
   * @return Returns the transform metadata
   */
  public TransformMeta getTransformMeta() {
    return transformMeta;
  }

  /**
   * @param transformMeta The transform metadata to set.
   */
  public void setTransformMeta( TransformMeta transformMeta ) {
    this.transformMeta = transformMeta;
  }

  /**
   * @return Returns the pipelineMeta.
   */
  public PipelineMeta getPipelineMeta() {
    return pipelineMeta;
  }

  /**
   * @param pipelineMeta The pipelineMeta to set.
   */
  public void setPipelineMeta( PipelineMeta pipelineMeta ) {
    this.pipelineMeta = pipelineMeta;
  }
}
