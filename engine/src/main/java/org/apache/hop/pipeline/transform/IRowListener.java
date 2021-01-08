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

package org.apache.hop.pipeline.transform;

import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;

/**
 * IRowListener is a listener interface for receiving row events. A class that is interested in processing a row event
 * implements this interface, and the object created with that class is registered with a component using the
 * component's
 *
 * <pre>
 * addRowListener
 * </pre>
 * <p>
 * method. When the row event occurs, that object's appropriate method is invoked.
 *
 */
public interface IRowListener {
  /**
   * This method is called when a row is read from another transform
   *
   * @param rowMeta the metadata of the row
   * @param row     the data of the row
   * @throws HopTransformException an exception that can be thrown to hard stop the transform
   */
  void rowReadEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException;

  /**
   * This method is called when a row is written to another transform (even if there is no next transform)
   *
   * @param rowMeta the metadata of the row
   * @param row     the data of the row
   * @throws HopTransformException an exception that can be thrown to hard stop the transform
   */
  void rowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException;

  /**
   * This method is called when the error handling of a row is writing a row to the error stream.
   *
   * @param rowMeta the metadata of the row
   * @param row     the data of the row
   * @throws HopTransformException an exception that can be thrown to hard stop the transform
   */
  void errorRowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException;
}
