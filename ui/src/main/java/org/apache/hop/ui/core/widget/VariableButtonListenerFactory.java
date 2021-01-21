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

package org.apache.hop.ui.core.widget;

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Shell;

import java.util.Arrays;

public class VariableButtonListenerFactory {
  private static final Class<?> PKG = VariableButtonListenerFactory.class; // For Translator

  // Listen to the Variable... button
  public static final SelectionAdapter getSelectionAdapter( final Composite composite, final TextVar destination,
                                                            final IVariables variables ) {
    return getSelectionAdapter( composite, destination, null, null, variables );
  }

  // Listen to the Variable... button
  public static final SelectionAdapter getSelectionAdapter( final Composite composite, final TextVar destination,
                                                            final IGetCaretPosition getCaretPositionInterface, final IInsertText insertTextInterface,
                                                            final IVariables variables ) {
    return new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        // Before focus is lost, we get the position of where the selected variable needs to be inserted.
        int position = 0;
        if ( getCaretPositionInterface != null ) {
          position = getCaretPositionInterface.getCaretPosition();
        }

        String variableName = getVariableName( composite.getShell(), variables );
        if ( variableName != null ) {
          String var = "${" + variableName + "}";
          if ( insertTextInterface == null ) {
            destination.getTextWidget().insert( var );
            e.doit = false;
          } else {
            insertTextInterface.insertText( var, position );
          }
        }
      }
    };
  }

  // Listen to the Variable... button
  public static final String getVariableName( Shell shell, IVariables variables ) {
    String[] keys = variables.getVariableNames();
    Arrays.sort( keys );

    int size = keys.length;
    String[] key = new String[ size ];
    String[] val = new String[ size ];
    String[] str = new String[ size ];

    for ( int i = 0; i < keys.length; i++ ) {
      key[ i ] = keys[ i ];
      val[ i ] = variables.getVariable( key[ i ] );
      str[ i ] = key[ i ] + "  [" + val[ i ] + "]";
    }

    EnterSelectionDialog esd = new EnterSelectionDialog( shell, str,
      BaseMessages.getString( PKG, "System.Dialog.SelectEnvironmentVar.Title" ),
      BaseMessages.getString( PKG, "System.Dialog.SelectEnvironmentVar.Message" ) );
    esd.clearModal();
    if ( esd.open() != null ) {
      int nr = esd.getSelectionNr();
      String var = key[ nr ];

      return var;
    } else {
      return null;
    }
  }
}
