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

import org.apache.hop.core.injection.BaseMetadataInjectionTest;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironment;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class ExcelInputMetaInjectionTest extends BaseMetadataInjectionTest<ExcelInputMeta> {
  @ClassRule public static RestoreHopEngineEnvironment env = new RestoreHopEngineEnvironment();

  @Before
  public void setup() throws Exception {
    setup( new ExcelInputMeta() );
  }

  @Test
  public void test() throws Exception {
    check( "NAME", () -> meta.getField()[ 0 ].getName() );
    check( "LENGTH", () -> meta.getField()[ 0 ].getLength() );
    check( "PRECISION", () -> meta.getField()[ 0 ].getPrecision() );
    int[] trimInts = new int[ ValueMetaBase.trimTypeCode.length ];
    for ( int i = 0; i < trimInts.length; i++ ) {
      trimInts[ i ] = i;
    }
    checkStringToInt( "TRIM_TYPE", () -> meta.getField()[ 0 ].getTrimType(), ValueMetaBase.trimTypeCode, trimInts );
    check( "FORMAT", () -> meta.getField()[ 0 ].getFormat() );
    check( "CURRENCY", () -> meta.getField()[ 0 ].getCurrencySymbol() );
    check( "DECIMAL", () -> meta.getField()[ 0 ].getDecimalSymbol() );
    check( "GROUP", () -> meta.getField()[ 0 ].getGroupSymbol() );
    check( "REPEAT", () -> meta.getField()[ 0 ].isRepeated() );

    // TODO check field type plugins
    skipPropertyTest( "TYPE" );

    check( "SHEET_NAME", () -> meta.getSheetName()[ 0 ] );
    check( "SHEET_START_ROW", () -> meta.getStartRow()[ 0 ] );
    check( "SHEET_START_COL", () -> meta.getStartColumn()[ 0 ] );
    check( "FILENAME", () -> meta.getFileName()[ 0 ] );
    check( "FILEMASK", () -> meta.getFileMask()[ 0 ] );
    check( "EXCLUDE_FILEMASK", () -> meta.getExcludeFileMask()[ 0 ] );
    check( "FILE_REQUIRED", () -> meta.getFileRequired()[ 0 ] );
    check( "INCLUDE_SUBFOLDERS", () -> meta.getIncludeSubFolders()[ 0 ] );
    check( "SPREADSHEET_TYPE", () -> meta.getSpreadSheetType(), SpreadSheetType.class );
  }
}
