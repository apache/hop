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

package org.apache.hop.core.compress.gzip;

import org.apache.hop.core.compress.CompressionOutputStream;
import org.apache.hop.core.compress.ICompressionProvider;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipOutputStream;

public class GzipCompressionOutputStream extends CompressionOutputStream {

  public GzipCompressionOutputStream( OutputStream out, ICompressionProvider provider ) throws IOException {
    super( getDelegate( out ), provider );

  }

  protected static GZIPOutputStream getDelegate( OutputStream out ) throws IOException {
    GZIPOutputStream delegate = null;
    if ( out instanceof ZipOutputStream ) {
      delegate = (GZIPOutputStream) out;
    } else {
      delegate = new GZIPOutputStream( out );
    }
    return delegate;
  }

  @Override
  public void close() throws IOException {
    GZIPOutputStream zos = (GZIPOutputStream) delegate;
    zos.close();
  }
}
