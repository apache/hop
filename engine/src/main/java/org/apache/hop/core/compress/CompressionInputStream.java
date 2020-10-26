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
import java.io.InputStream;

public abstract class CompressionInputStream extends InputStream {

  private ICompressionProvider compressionProvider;
  protected InputStream delegate;

  public CompressionInputStream( InputStream in, ICompressionProvider provider ) {
    this();
    delegate = in;
    compressionProvider = provider;
  }

  private CompressionInputStream() {
    super();
  }

  public ICompressionProvider getCompressionProvider() {
    return compressionProvider;
  }

  public Object nextEntry() throws IOException {
    return null;
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public int read() throws IOException {
    return delegate.read();
  }

  @Override
  public int read( byte[] b ) throws IOException {
    return delegate.read( b );
  }

  @Override
  public int read( byte[] b, int off, int len ) throws IOException {
    return delegate.read( b, off, len );
  }
}
