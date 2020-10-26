/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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

package org.apache.hop.core.spreadsheet;

public enum KCellType {
  EMPTY( "Empty" ), BOOLEAN( "Boolean" ), BOOLEAN_FORMULA( "Boolean formula" ), DATE( "Date" ), DATE_FORMULA(
    "Date formula" ), LABEL( "Label" ), STRING_FORMULA( "String formula" ), NUMBER( "Number" ), NUMBER_FORMULA(
    "Number formula" );

  private String description;

  private KCellType( String description ) {
    this.description = description;
  }

  public String getDescription() {
    return description;
  }
}
