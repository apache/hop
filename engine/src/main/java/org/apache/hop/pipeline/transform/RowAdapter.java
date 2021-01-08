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
 * RowAdapter is an adapter class for receiving row events. The methods in this class are empty. This class exists as
 * convenience for creating row listener objects that may not need to implement all the methods of the IRowListener
 * interface
 *
 * @see IRowListener
 */
public class RowAdapter implements IRowListener {

  /**
   * Instantiates a new row adapter.
   */
  public RowAdapter() {
  }

  /**
   * Empty method implementing the IRowListener.errorRowWrittenEvent interface method
   *
   * @see IRowListener#errorRowWrittenEvent(IRowMeta,
   * java.lang.Object[])
   */
  public void errorRowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
  }

  /**
   * Empty method implementing the IRowListener.rowReadEvent interface method
   *
   * @see IRowListener#rowReadEvent(IRowMeta,
   * java.lang.Object[])
   */
  public void rowReadEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
  }

  /**
   * Empty method implementing the IRowListener.rowWrittenEvent interface method
   *
   * @see IRowListener#rowWrittenEvent(IRowMeta,
   * java.lang.Object[])
   */
  public void rowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
  }

}
