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

package org.apache.hop.trans.step;

import org.apache.hop.metastore.api.IMetaStore;

import java.awt.*;

/**
 * This interface is used to launch Step Dialogs. All dialogs that implement this simple interface can be opened by
 * Spoon.
 *
 * @author Matt
 * @since 4-aug-2004
 */
public interface StepDialogInterface {

  /**
   * Opens a step dialog window.
   *
   * @return the (potentially new) name of the step
   */
  String open();

  /**
  void populateComposite( Composite parent);

  /**
   * @param metaStore The MetaStore to pass
   */
  void setMetaStore( IMetaStore metaStore );
}
