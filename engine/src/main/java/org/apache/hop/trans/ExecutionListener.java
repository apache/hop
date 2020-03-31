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
import org.apache.hop.trans.engine.IPipelineEngine;

/**
 * A listener for execution engines
 *
 * @param <T> The executing object/subject (TransMeta)
 */
public interface ExecutionListener<T> {

  /**
   * The execution started
   *
   * @param engine
   * @throws HopException
   */
  void started( IPipelineEngine<T> engine ) throws HopException;

  /**
   * This transformation went from an in-active to an active state.
   *
   * @param engine
   * @throws HopException
   */
  public void becameActive( IPipelineEngine<T> engine );

  /**
   * The transformation has finished.
   *
   * @param engine
   * @throws HopException
   */
  public void finished( IPipelineEngine<T> engine ) throws HopException;
}
