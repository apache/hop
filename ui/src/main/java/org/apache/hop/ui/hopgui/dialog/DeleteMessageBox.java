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

import org.apache.hop.core.Const;
import org.apache.hop.i18n.BaseMessages;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

import java.util.Iterator;
import java.util.List;

/**
 * Displays the delete message box to confirm deletes of multiple transforms or actions
 *
 * @author David Kincade
 */
public class DeleteMessageBox extends MessageBox {
  private static final Class<?> PKG = DeleteMessageBox.class; // For Translator

  // The title for the message box
  private static final String title = BaseMessages.getString( PKG, "DeleteMessageBox.Title" );

  // The text to display in the dialog
  private String text = null;

  // The list of proposed transforms to be deleted
  private List<String> transformList = null;

  /**
   * Creates a message box to confirm the deletion of the items
   *
   * @param shell    the shell which will be the parent of the new instance
   * @param text     the title for the dialog
   * @param transformList the text list of proposed transforms to be deleted
   */
  public DeleteMessageBox( Shell shell, String text, List<String> transformList ) {
    super( shell, SWT.YES | SWT.NO | SWT.ICON_WARNING );
    this.text = text;
    this.transformList = transformList;
  }

  /**
   * Creats the dialog and then performs the display and returns the result
   *
   * @see org.eclipse.swt.widgets.MessageBox
   */
  public int open() {
    // Set the title
    setText( title );

    // Set the message
    setMessage( buildMessage() );

    // Perform the normal open operation
    return super.open();
  }

  /**
   * Builds a message from the text and the transformList
   *
   * @return
   */
  protected String buildMessage() {
    StringBuilder sb = new StringBuilder();
    sb.append( text ).append( Const.CR );
    if ( transformList != null ) {
      for ( Iterator<String> it = transformList.iterator(); it.hasNext(); ) {
        sb.append( "  - " ).append( it.next() ).append( Const.CR );
      }
    }
    return sb.toString();
  }

  /**
   * Allow this class to subclass MessageBox
   */
  protected void checkSubclass() {
  }
}
