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

package org.apache.hop.utils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

public class TestUtils {


  /**
   * Do not use this method because it does not delete the temp folder after java process tear down
   */
  @Deprecated
  public static String createTempDir() {
    String ret = null;
    try {
      /*
       * Java.io.File only creates Temp files, so repurpose the filename for a temporary folder
       * Delete the file that's created, and re-create as a folder.
       */
      File file = File.createTempFile( "temp_hop_test_dir", String.valueOf( System.currentTimeMillis() ) );
      file.delete();
      file.mkdir();
      file.deleteOnExit();
      ret = file.getAbsolutePath();
    } catch ( Exception ex ) {
      ex.printStackTrace();
    }
    return ret;
  }

  public static File getInputFile( String prefix, String suffix ) throws IOException {
    File inputFile = File.createTempFile( prefix, suffix );
    inputFile.deleteOnExit();
    FileUtils.writeStringToFile( inputFile, UUID.randomUUID().toString(), "UTF-8" );
    return inputFile;
  }

  public static String createRamFile( String path ) {
    return createRamFile( path, null );
  }

  public static String createRamFile( String path, IVariables variables ) {
    if ( variables == null ) {
      variables = new Variables();
      variables.initializeFrom( null );
    }
    try {
      FileObject file = HopVfs.getFileObject( "ram://" + path );
      file.createFile();
      return file.getName().getURI();
    } catch ( FileSystemException | HopFileException e ) {
      throw new RuntimeException( e );
    }
  }

  public static FileObject getFileObject( String vfsPath ) {
    return getFileObject( vfsPath, null );
  }

  public static FileObject getFileObject( String vfsPath, IVariables variables ) {
    if ( variables == null ) {
      variables = new Variables();
      variables.initializeFrom( null );
    }
    try {
      return HopVfs.getFileObject( vfsPath );
    } catch ( HopFileException e ) {
      throw new RuntimeException( e );
    }
  }

  public static String toUnixLineSeparators( String string ) {
    if ( string != null ) {
      string = string.replaceAll( "\r", "" );
    }
    return string;
  }

  public static void checkEqualsHashCodeConsistency( Object object1, Object object2 ) {
    if ( object1.equals( object2 ) ) {
      assertTrue( "inconsistent hashcode and equals", object1.hashCode() == object2.hashCode() );
    }
  }
}
