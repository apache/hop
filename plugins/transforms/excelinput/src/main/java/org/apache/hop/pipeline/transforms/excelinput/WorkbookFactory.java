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
import org.apache.hop.core.spreadsheet.IKWorkbook;
import org.apache.hop.pipeline.transforms.excelinput.ods.OdfWorkbook;
import org.apache.hop.pipeline.transforms.excelinput.poi.PoiWorkbook;
import org.apache.hop.pipeline.transforms.excelinput.staxpoi.StaxPoiWorkbook;

import java.io.InputStream;

public class WorkbookFactory {
  
  private WorkbookFactory() {
    throw new IllegalStateException("Utility class");
  }

  public static IKWorkbook getWorkbook( SpreadSheetType type, String filename, String encoding ) throws HopException {
    switch ( type ) {

      case POI:
        return new PoiWorkbook( filename, encoding ); // encoding is not used, perhaps detected automatically?
      case SAX_POI:
        return new StaxPoiWorkbook( filename, encoding );
      case ODS:
        return new OdfWorkbook( filename, encoding ); // encoding is not used, perhaps detected automatically?
      default:
        throw new HopException( "Sorry, spreadsheet type " + type.getDescription() + " is not yet supported" );
    }

  }

  public static IKWorkbook getWorkbook( SpreadSheetType type, InputStream inputStream, String encoding ) throws HopException {
    switch ( type ) {

      case POI:
        return new PoiWorkbook( inputStream, encoding ); // encoding is not used, perhaps detected automatically?
      case SAX_POI:
        return new StaxPoiWorkbook( inputStream, encoding );
      case ODS:
        return new OdfWorkbook( inputStream, encoding ); // encoding is not used, perhaps detected automatically?
      default:
        throw new HopException( "Sorry, spreadsheet type " + type.getDescription() + " is not yet supported" );
    }

  }
}
