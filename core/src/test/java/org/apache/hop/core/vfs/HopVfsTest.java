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

package org.apache.hop.core.vfs;


import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopFileException;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class HopVfsTest {

  /**
   * Test to validate that startsWitScheme() returns true if the fileName starts with
   * known protocol like zip: jar: then it returns true else returns false
   */
  @Test
  public void testStartsWithScheme() {
    String fileName = "zip:file:///SavedLinkedres.zip!Calculate median and percentiles using the group by transforms.hpl";
    assertTrue( HopVfs.startsWithScheme( fileName ) );

    fileName = "SavedLinkedres.zip!Calculate median and percentiles using the group by transforms.hpl";
    assertFalse( HopVfs.startsWithScheme( fileName ) );
  }


  @Test
  public void testCheckForSchemeSuccess() throws HopFileException, IOException {
    String[] schemes = { "hdfs" };
    String vfsFilename = "hdfs://company.com:8020/tmp/acltest/";

    boolean test = HopVfs.checkForScheme( schemes, true, vfsFilename );
    assertFalse( test );

  }

  @Test
  public void testCheckForSchemeFail() throws HopFileException, IOException {
    String[] schemes = { "file" };
    String vfsFilename = "hdfs://company.com:8020/tmp/acltest/";

    boolean test = HopVfs.checkForScheme( schemes, true, vfsFilename );
    assertTrue( test );

  }

  @Test
  public void testRamFilesCache() throws Exception {
    String filename = "ram:///test-file.txt";
    FileObject fileObject = HopVfs.getFileObject( filename );
    try (OutputStream outputStream = fileObject.getContent().getOutputStream() ) {
      outputStream.write( "Test-content".getBytes() );
    }
  }
}
