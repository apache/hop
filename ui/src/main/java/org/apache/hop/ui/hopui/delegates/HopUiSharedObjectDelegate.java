/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.ui.hopui.delegates;


import org.apache.hop.core.EngineMetaInterface;
import org.apache.hop.core.changed.ChangedFlagInterface;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.shared.SharedObjectInterface;
import org.apache.hop.ui.hopui.HopUi;
import org.apache.hop.ui.hopui.SharedObjectSyncUtil;

import java.util.List;

public abstract class HopUiSharedObjectDelegate extends HopUiDelegate {
  protected static final Class<?> PKG = HopUi.class;
  protected SharedObjectSyncUtil sharedObjectSyncUtil;

  public HopUiSharedObjectDelegate( HopUi hopUi ) {
    super( hopUi );
  }


  public void setSharedObjectSyncUtil( SharedObjectSyncUtil sharedObjectSyncUtil ) {
    this.sharedObjectSyncUtil = sharedObjectSyncUtil;
  }

  protected static boolean isDuplicate( List<? extends SharedObjectInterface> objects, SharedObjectInterface object ) {
    String newName = object.getName();
    for ( SharedObjectInterface soi : objects ) {
      if ( soi.getName().equalsIgnoreCase( newName ) ) {
        return true;
      }
    }
    return false;
  }

  protected void saveSharedObjects() {
    try {
      // flush to file for newly opened
      EngineMetaInterface meta = hopUi.getActiveMeta();
      if ( meta != null ) {
        meta.saveSharedObjects();
      }
    } catch ( HopException e ) {
      hopUi.getLog().logError( e.getLocalizedMessage(), e );
    }
  }

  protected static String getMessage( String key ) {
    return BaseMessages.getString( PKG, key );
  }

  protected static String getMessage( String key, Object... params ) {
    return BaseMessages.getString( PKG, key, params );
  }

}
