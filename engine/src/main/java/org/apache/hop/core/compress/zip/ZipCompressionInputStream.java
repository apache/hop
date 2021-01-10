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

import org.apache.hop.core.compress.CompressionInputStream;
import org.apache.hop.core.compress.ICompressionProvider;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipInputStream;

public class ZipCompressionInputStream extends CompressionInputStream {

  public ZipCompressionInputStream( InputStream in, ICompressionProvider provider ) throws IOException {
    super( getDelegate( in ), provider );
  }

  protected static ZipInputStream getDelegate( InputStream in ) throws IOException {
    ZipInputStream delegate = null;
    if ( in instanceof ZipInputStream ) {
      delegate = (ZipInputStream) in;
    } else {
      delegate = new ZipInputStream( in );
    }
    return delegate;
  }

  @Override
  public void close() throws IOException {
    ZipInputStream zis = (ZipInputStream) delegate;
    if ( zis == null ) {
      throw new IOException( "Not a valid input stream!" );
    }
    zis.close();
  }

  @Override
  public int read() throws IOException {
    ZipInputStream zis = (ZipInputStream) delegate;
    if ( zis == null ) {
      throw new IOException( "Not a valid input stream!" );
    }
    return zis.read();
  }

  @Override
  public Object nextEntry() throws IOException {
    ZipInputStream zis = (ZipInputStream) delegate;
    if ( zis == null ) {
      throw new IOException( "Not a valid input stream!" );
    }
    return zis.getNextEntry();
  }

}
