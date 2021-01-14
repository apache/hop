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

package org.apache.hop.pipeline.transforms.excelinput;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;

/**
 * Extended {@link HopException} to allow passing of extra context info up the chain (sheet, row, and column IDs).
 * <p>
 * If we were really obsessive, we'd cache both the names and indexes of all the items, including the input file. But
 * this will do for a start.
 *
 * @author timh
 * @since 14-FEB-2008
 */

public class HopCellValueException extends HopException {

  private static final Class<?> PKG = ExcelInputMeta.class; // For Translator

  private static final long serialVersionUID = 1L;

  private final int sheetnr;
  private final int rownr;
  private final int colnr;
  private final String fieldName;

  /**
   * Standard constructor.
   * <p/>
   * <em>Note:</em> All indexes below have a 0-origin (internal index), but are reported with a 1-origin (human index).
   *
   * @param ex        The Exception to wrap.
   * @param sheetnr   Sheet number
   * @param rownr     Row number
   * @param colnr     Column number
   * @param fieldName The name of the field being converted
   */
  public HopCellValueException( HopException ex, int sheetnr, int rownr, int colnr, String fieldName ) {
    super( ex );
    // Note that internal indexes start at 0
    this.sheetnr = sheetnr + 1;
    this.rownr = rownr + 1;
    this.colnr = colnr + 1;
    this.fieldName = fieldName;
  }

  @Override
  public String getMessage() {
    String msgText =
      BaseMessages.getString( PKG, "HopCellValueException.CannotConvertFieldFromCell", Integer
        .toString( sheetnr ), Integer.toString( rownr ), Integer.toString( colnr ), fieldName, super
        .getMessage() );
    return msgText;
  }

}
