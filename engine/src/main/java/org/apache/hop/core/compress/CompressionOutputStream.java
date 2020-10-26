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

package org.apache.hop.core.compress;

import java.io.IOException;
import java.io.OutputStream;

public abstract class CompressionOutputStream extends OutputStream {

  private ICompressionProvider compressionProvider;
  protected OutputStream delegate;

  public CompressionOutputStream( OutputStream out, ICompressionProvider provider ) {
    this();
    delegate = out;
    compressionProvider = provider;
  }

  private CompressionOutputStream() {
    super();
  }

  public ICompressionProvider getCompressionProvider() {
    return compressionProvider;
  }

  public void addEntry( String filename, String extension ) throws IOException {
    // Default no-op behavior
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public void write( int b ) throws IOException {
    delegate.write( b );
  }

  @Override
  public void write( byte[] b ) throws IOException {
    delegate.write( b );
  }

  @Override
  public void write( byte[] b, int off, int len ) throws IOException {
    delegate.write( b, off, len );
  }
}
