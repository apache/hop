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

package org.apache.hop.pipeline.transforms.salesforce;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.apache.hop.core.variables.IVariables;

public abstract class SalesforceTransformDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = SalesforceTransformMeta.class; // For Translator

  protected static final String DEFAULT_DATE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'.000'Z";
  protected static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";

  private final Class<? extends SalesforceTransformMeta> META_CLASS;

  public SalesforceTransformDialog( Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname ) {
    super( parent, variables, (BaseTransformMeta) in, pipelineMeta, sname );
    META_CLASS = ( (SalesforceTransformMeta) in ).getClass();
  }

  protected abstract void getInfo( SalesforceTransformMeta meta ) throws HopException;

  protected void test() {

    boolean successConnection = true;
    String msgError = null;
    SalesforceConnection connection = null;
    String realUsername = null;
    try {
      SalesforceTransformMeta meta = META_CLASS.newInstance();
      getInfo( meta );

      // get real values
      String realURL = variables.resolve( meta.getTargetUrl() );
      realUsername = variables.resolve( meta.getUsername() );
      String realPassword = Utils.resolvePassword( variables, meta.getPassword() );
      int realTimeOut = Const.toInt( variables.resolve( meta.getTimeout() ), 0 );

      connection = new SalesforceConnection( log, realURL, realUsername, realPassword );
      connection.setTimeOut( realTimeOut );
      connection.connect();

    } catch ( Exception e ) {
      successConnection = false;
      msgError = e.getMessage();
    } finally {
      if ( connection != null ) {
        try {
          connection.close();
        } catch ( Exception e ) { /* Ignore */
        }
      }
    }
    if ( successConnection ) {

      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_INFORMATION );
      mb.setMessage( BaseMessages.getString( PKG, "SalesforceTransformDialog.Connected.OK", realUsername )
        + Const.CR );
      mb.setText( BaseMessages.getString( PKG, "SalesforceTransformDialog.Connected.Title.Ok" ) );
      mb.open();
    } else {
      new ErrorDialog(
        shell,
        BaseMessages.getString( PKG, "SalesforceTransformDialog.Connected.Title.Error" ),
        BaseMessages.getString( PKG, "SalesforceTransformDialog.Connected.NOK", realUsername ),
        new Exception( msgError ) );
    }
  }
}
