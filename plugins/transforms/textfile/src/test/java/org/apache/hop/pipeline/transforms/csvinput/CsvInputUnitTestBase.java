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

package org.apache.hop.pipeline.transforms.csvinput;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.file.TextFileInputField;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * @author Andrey Khayrutdinov
 */
public abstract class CsvInputUnitTestBase {

  static final String BUFFER_SIZE = "1024";
  static final String ENCODING = "utf-8";
  static final String ENCLOSURE = "\"";
  static final String DELIMITER = ",";

  @BeforeClass
  public static void initHop() throws Exception {
    HopEnvironment.init();
  }

  File createTestFile( final String encoding, final String content ) throws IOException {
    File tempFile = File.createTempFile( "PDI_tmp", ".tmp" );
    tempFile.deleteOnExit();

    try ( PrintWriter osw = new PrintWriter( tempFile, encoding ) ) {
      osw.write( content );
    }

    return tempFile;
  }

  TextFileInputField[] createInputFileFields( String... names ) {
    TextFileInputField[] fields = new TextFileInputField[ names.length ];
    for ( int i = 0; i < names.length; i++ ) {
      fields[ i ] = createField( names[ i ] );
    }
    return fields;
  }

  TextFileInputField createField( String name ) {
    TextFileInputField field = new TextFileInputField();
    field.setName( name );
    field.setType( IValueMeta.TYPE_STRING );
    return field;
  }

  CsvInputMeta createMeta( File file, TextFileInputField[] fields ) {
    CsvInputMeta meta = new CsvInputMeta();
    meta.setFilename( file.getAbsolutePath() );
    meta.setBufferSize( BUFFER_SIZE );
    meta.setDelimiter( DELIMITER );
    meta.setEnclosure( ENCLOSURE );
    meta.setEncoding( ENCODING );
    meta.setInputFields( fields );
    meta.setHeaderPresent( false );
    return meta;
  }
}
