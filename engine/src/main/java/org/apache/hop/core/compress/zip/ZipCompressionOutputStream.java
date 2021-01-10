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

package org.apache.hop.core.compress.zip;

import org.apache.hop.core.Const;
import org.apache.hop.core.compress.CompressionOutputStream;
import org.apache.hop.core.compress.ICompressionProvider;
import org.apache.hop.core.util.Utils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ZipCompressionOutputStream extends CompressionOutputStream {

  public ZipCompressionOutputStream( OutputStream out, ICompressionProvider provider ) {
    super( getDelegate( out ), provider );
  }

  protected static ZipOutputStream getDelegate( OutputStream out ) {
    ZipOutputStream delegate;
    if ( out instanceof ZipOutputStream ) {
      delegate = (ZipOutputStream) out;
    } else {
      delegate = new ZipOutputStream( out );
    }
    return delegate;
  }

  @Override
  public void close() throws IOException {
    ZipOutputStream zos = (ZipOutputStream) delegate;
    zos.flush();
    zos.closeEntry();
    zos.finish();
    zos.close();
  }

  @Override
  public void addEntry( String filename, String extension ) throws IOException {
    // remove folder hierarchy
    int index = filename.lastIndexOf( Const.FILE_SEPARATOR );
    String entryPath;
    if ( index != -1 ) {
      entryPath = filename.substring( index + 1 );
    } else {
      entryPath = filename;
    }

    // remove ZIP extension
    index = entryPath.toLowerCase().lastIndexOf( ".zip" );
    if ( index != -1 ) {
      entryPath = entryPath.substring( 0, index ) + entryPath.substring( index + ".zip".length() );
    }

    // add real extension if needed
    if ( !Utils.isEmpty( extension ) ) {
      entryPath += "." + extension;
    }

    ZipEntry zipentry = new ZipEntry( entryPath );
    zipentry.setComment( "Compressed by Hop" );
    ( (ZipOutputStream) delegate ).putNextEntry( zipentry );
  }
}
