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

package org.apache.hop.trans;

import org.apache.hop.core.exception.HopException;

public interface TransListener {

  /**
   * This transformation started
   *
   * @param trans
   * @throws HopException
   */
  void transStarted( Trans trans ) throws HopException;

  /**
   * This transformation went from an in-active to an active state.
   *
   * @param trans
   * @throws HopException
   */
  public void transActive( Trans trans );

  /**
   * The transformation has finished.
   *
   * @param trans
   * @throws HopException
   */
  public void transFinished( Trans trans ) throws HopException;
}
