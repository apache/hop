/*
 * ! ******************************************************************************
 *
 *  Pentaho Data Integration
 *
 *  Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 * ******************************************************************************
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 * *****************************************************************************
 */

package org.apache.hop.pipeline.transform;

import org.apache.hop.core.RowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.i18n.BaseMessages;

/**
 * Defines methods used for handling row data within transforms.
 * <p>
 * By default, the implementation used in BaseTransform leverages
 * the logic defined within BaseTransform.
 * (see {@link BaseTransform#handleGetRow()}
 * {@link BaseTransform#handlePutRow(RowMetaInterface, Object[])}
 * {@link BaseTransform#handlePutError }
 * <p>
 * {@link BaseTransform#setRowHandler(RowHandler) } can be used to override
 * this behavior.
 */
public interface RowHandler {
  Class<?> PKG = BaseTransform.class;

  Object[] getRow() throws HopException;

  void putRow( RowMetaInterface rowMeta, Object[] row ) throws HopTransformException;

  void putError( RowMetaInterface rowMeta, Object[] row, long nrErrors, String errorDescriptions,
                 String fieldNames, String errorCodes ) throws HopTransformException;

  default void putRowTo( RowMetaInterface rowMeta, Object[] row, RowSet rowSet )
    throws HopTransformException {
    throw new UnsupportedOperationException(
      BaseMessages.getString( PKG, "BaseTransform.RowHandler.PutRowToNotSupported",
        this.getClass().getName() ) );
  }

  default Object[] getRowFrom( RowSet rowSet ) throws HopTransformException {
    throw new UnsupportedOperationException(
      BaseMessages.getString( PKG, "BaseTransform.RowHandler.GetRowFromNotSupported",
        this.getClass().getName() ) );
  }

}
