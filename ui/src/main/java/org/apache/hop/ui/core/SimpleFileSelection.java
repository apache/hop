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

package org.apache.hop.ui.core;

import org.apache.commons.io.FilenameUtils;
import org.apache.hop.core.util.Assert;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Shell;

public class SimpleFileSelection extends SelectionAdapter {

  /**
   * The default filter extension.
   */
  public static final String DEFAULT_FILTER_EXTENSION = "*";

  /**
   * The default file extension.
   */
  public static final String DEFAULT_FILTER_NAME = "All files (*.*)";

  private final Shell shell;

  private final TextVar textVar;

  private final String[] filterExtensions;

  private final String[] filterNames;

  /**
   * Constructor.
   *
   * @param shell            shell to set.
   * @param textVar          text variable to edit.
   * @param filterExtensions filter extensions to set.
   * @param filterNames      filter names to set.
   * @throws IllegalArgumentException if shell or text variable is null.
   */
  public SimpleFileSelection( final Shell shell, final TextVar textVar, final String[] filterExtensions,
                              final String[] filterNames ) throws IllegalArgumentException {
    super();
    Assert.assertNotNull( shell, "Shell cannot be null" );
    Assert.assertNotNull( textVar, "Text var cannot be null" );
    Assert.assertNotNull( filterNames, "Filter names cannot be null" );
    Assert.assertNotNull( filterExtensions, "Filter extensions cannot be null" );
    this.shell = shell;
    this.textVar = textVar;
    this.filterExtensions = new String[ filterExtensions.length ];
    System.arraycopy( filterExtensions, 0, this.filterExtensions, 0, filterExtensions.length );
    this.filterNames = new String[ filterNames.length ];
    System.arraycopy( filterNames, 0, this.filterNames, 0, filterNames.length );
  }

  /**
   * Constructor.
   *
   * @param shell   the shell to set.
   * @param textVar the text variable to edit.
   * @throws IllegalArgumentException if shell or text variable is null.
   */
  public SimpleFileSelection( final Shell shell, final TextVar textVar ) throws IllegalArgumentException {
    this( shell, textVar, new String[] { DEFAULT_FILTER_EXTENSION }, new String[] { DEFAULT_FILTER_NAME } );
  }

  /**
   * Constructor.
   *
   * @param shell       the shell to set.
   * @param textVar     the text variable to edit.
   * @param filterNames the filter names to use.
   * @throws IllegalArgumentException if shell or text variable is null.
   */
  public SimpleFileSelection( final Shell shell, final TextVar textVar, final String... filterNames ) throws IllegalArgumentException {
    this( shell, textVar, new String[] { DEFAULT_FILTER_EXTENSION }, filterNames );
  }

  /**
   * {@inheritDoc}
   *
   * @see org.eclipse.swt.events.SelectionAdapter#widgetSelected(org.eclipse.swt.events.SelectionEvent)
   */
  @Override
  public void widgetSelected( final SelectionEvent event ) {
    BaseDialog.presentFileDialog( shell, textVar, null, null, filterExtensions, filterNames, true );

  }

}
