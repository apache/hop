/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 * http://www.project-hop.org
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

package org.apache.hop.core.compress.gzip;

import org.apache.hop.core.compress.CompressionInputStream;
import org.apache.hop.core.compress.ICompressionProvider;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

public class GzipCompressionInputStream extends CompressionInputStream {

  public GzipCompressionInputStream( InputStream in, ICompressionProvider provider ) throws IOException {
    super( getDelegate( in ), provider );
  }

  protected static GZIPInputStream getDelegate( InputStream in ) throws IOException {
    GZIPInputStream delegate = null;
    if ( in instanceof GZIPInputStream ) {
      delegate = (GZIPInputStream) in;
    } else {
      delegate = new GZIPInputStream( in );
    }
    return delegate;
  }

  @Override
  public void close() throws IOException {
    GZIPInputStream gis = (GZIPInputStream) delegate;
    gis.close();
  }

  @Override
  public int read() throws IOException {
    GZIPInputStream gis = (GZIPInputStream) delegate;
    return gis.read();
  }

}
