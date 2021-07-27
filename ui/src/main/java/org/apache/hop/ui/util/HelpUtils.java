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

package org.apache.hop.ui.util;

import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.gui.GuiResource;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.program.Program;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

public class HelpUtils {
  private static final Class<?> PKG = HelpUtils.class; // For Translator

  public static Button createHelpButton( final Composite parent, final IPlugin plugin ) {
    Button button = newButton( parent );
    button.addListener(SWT.Selection, e -> openHelp( parent.getShell(), plugin ));    
    return button;
  }
  
  public static Button createHelpButton( final Composite parent, final String url ) {
    Button button = newButton( parent );
    button.addListener(SWT.Selection, e -> Program.launch(url));   
    return button;
  }

  private static Button newButton( final Composite parent ) {
    Button button = new Button( parent, SWT.PUSH );
    button.setImage( GuiResource.getInstance().getImageHelpWeb() );
    button.setText( BaseMessages.getString( PKG, "System.Button.Help" ) );
    button.setToolTipText( BaseMessages.getString( PKG, "System.Tooltip.Help" ) );
    FormData fdButton = new FormData();
    fdButton.left = new FormAttachment( 0, 0 );
    fdButton.bottom = new FormAttachment( 100, 0 );
    button.setLayoutData( fdButton );
    return button;
  }

  public static boolean isPluginDocumented( IPlugin plugin ) {
    if ( plugin == null ) {
      return false;
    }
    return !StringUtil.isEmpty( plugin.getDocumentationUrl() );
  }

  public static void openHelp(Shell shell, IPlugin plugin) {
    if ( shell == null || plugin == null ) {
      return;
    }
    if ( isPluginDocumented( plugin ) ) {
      Program.launch(plugin.getDocumentationUrl());
    } else {
      MessageBox mb = new MessageBox( shell, SWT.OK | SWT.ICON_ERROR );
      String msg = "";
      // TODO currently support only Transform, Action and Metadata - extend if required.
      if ( plugin.getPluginType().equals( TransformPluginType.class ) ) {
        msg = BaseMessages.getString( PKG, "System.Help.Transform.IsNotAvailable", plugin.getName());
      } else if ( plugin.getPluginType().equals( ActionPluginType.class ) ) {
        msg = BaseMessages.getString( PKG, "System.Help.Action.IsNotAvailable", plugin.getName());
      } else {
        msg = BaseMessages.getString( PKG, "System.Help.Metadata.IsNotAvailable", plugin.getName());
      }
      
      mb.setMessage(msg);
      mb.setText( BaseMessages.getString( PKG, "System.Dialog.Error.Title" ) );
      mb.open();
    }
  }
}
