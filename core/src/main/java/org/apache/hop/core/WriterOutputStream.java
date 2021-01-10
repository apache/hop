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

package org.apache.hop.core;

import org.apache.hop.core.util.Utils;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;

/**
 * This class provides a simple wrapper to disguise a Writer as an OutputStream.
 *
 * @author matt
 */
public class WriterOutputStream extends OutputStream {
  private Writer writer;
  private String encoding;

  public WriterOutputStream( Writer writer ) {
    this.writer = writer;
  }

  public WriterOutputStream( Writer writer, String encoding ) {
    this.writer = writer;
    this.encoding = encoding;
  }

  @Override
  public void write( int b ) throws IOException {
    write( new byte[] { (byte) b, } );
  }

  @Override
  public void write( byte[] b, int off, int len ) throws IOException {
    byte[] buf = new byte[ len ];
    System.arraycopy( b, off, buf, 0, len );
    write( buf );
  }

  @Override
  public void write( byte[] b ) throws IOException {
    if ( Utils.isEmpty( encoding ) ) {
      writer.append( new String( b ) );
    } else {
      writer.append( new String( b, encoding ) );
    }
  }

  @Override
  public void close() throws IOException {
    writer.close();
    writer = null;
  }

  @Override
  public void flush() throws IOException {
    writer.flush();
  }

  public Writer getWriter() {
    return writer;
  }

  public String getEncoding() {
    return encoding;
  }
}
