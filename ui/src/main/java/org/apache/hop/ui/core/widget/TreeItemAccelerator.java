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

import org.apache.hop.ui.core.ConstUi;
import org.eclipse.swt.events.DisposeEvent;
import org.eclipse.swt.events.DisposeListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

/**
 * This class can be used to define accelerators (actions) to a tree item that just got created.
 *
 * @author Matt
 */
public class TreeItemAccelerator {
  public static final void addDoubleClick( final TreeItem treeItem, final IDoubleClick doubleClick ) {
    final String[] path1 = ConstUi.getTreeStrings( treeItem );
    final Tree tree = treeItem.getParent();

    if ( doubleClick != null ) {
      final SelectionAdapter selectionAdapter = new SelectionAdapter() {
        public void widgetDefaultSelected( SelectionEvent selectionEvent ) {
          TreeItem[] items = tree.getSelection();
          for ( int i = 0; i < items.length; i++ ) {
            String[] path2 = ConstUi.getTreeStrings( items[ i ] );
            if ( equalPaths( path1, path2 ) ) {
              doubleClick.action( treeItem );
            }
          }
        }
      };
      tree.addSelectionListener( selectionAdapter );

      // Clean up when we do a refresh too.
      treeItem.addDisposeListener( disposeEvent -> tree.removeSelectionListener( selectionAdapter ) );
    }
  }

  public static final boolean equalPaths( String[] path1, String[] path2 ) {
    if ( path1 == null || path2 == null ) {
      return false;
    }
    if ( path1.length != path2.length ) {
      return false;
    }

    for ( int i = 0; i < path1.length; i++ ) {
      if ( !path1[ i ].equals( path2[ i ] ) ) {
        return false;
      }
    }
    return true;
  }
}
