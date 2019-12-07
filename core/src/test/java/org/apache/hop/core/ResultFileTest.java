/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.vfs.HopVFS;
import org.apache.hop.junit.rules.RestoreHopEnvironment;

public class ResultFileTest {

  @ClassRule public static RestoreHopEnvironment env = new RestoreHopEnvironment();

  @Test
  public void testGetRow() throws HopFileException, FileSystemException {
    File tempDir = new File( new TemporaryFolder().toString() );
    FileObject tempFile = HopVFS.createTempFile( "prefix", "suffix", tempDir.toString() );
    Date timeBeforeFile = Calendar.getInstance().getTime();
    ResultFile resultFile = new ResultFile( ResultFile.FILE_TYPE_GENERAL, tempFile, "myOriginParent", "myOrigin" );
    Date timeAfterFile = Calendar.getInstance().getTime();

    assertNotNull( resultFile );
    RowMetaInterface rm = resultFile.getRow().getRowMeta();
    assertEquals( 7, rm.getValueMetaList().size() );
    assertEquals( ValueMetaInterface.TYPE_STRING, rm.getValueMeta( 0 ).getType() );
    assertEquals( ValueMetaInterface.TYPE_STRING, rm.getValueMeta( 1 ).getType() );
    assertEquals( ValueMetaInterface.TYPE_STRING, rm.getValueMeta( 2 ).getType() );
    assertEquals( ValueMetaInterface.TYPE_STRING, rm.getValueMeta( 3 ).getType() );
    assertEquals( ValueMetaInterface.TYPE_STRING, rm.getValueMeta( 4 ).getType() );
    assertEquals( ValueMetaInterface.TYPE_STRING, rm.getValueMeta( 5 ).getType() );
    assertEquals( ValueMetaInterface.TYPE_DATE, rm.getValueMeta( 6 ).getType() );

    assertEquals( ResultFile.FILE_TYPE_GENERAL, resultFile.getType() );
    assertEquals( "myOrigin", resultFile.getOrigin() );
    assertEquals( "myOriginParent", resultFile.getOriginParent() );
    assertTrue( "ResultFile timestamp is created in the expected window",
      timeBeforeFile.compareTo( resultFile.getTimestamp() ) <= 0
      && timeAfterFile.compareTo( resultFile.getTimestamp() ) >= 0 );

    tempFile.delete();
    tempDir.delete();
  }
}
