/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.vfs.azure;

import com.microsoft.azure.storage.blob.BlobInputStream;

import java.io.IOException;
import java.io.InputStream;

public class PageBlobInputStream extends InputStream {

  private BlobInputStream inputStream;
  private long fileSize;
  private long totalRead = 0;

  public PageBlobInputStream( BlobInputStream inputStream, long fileSize ) {
    this.inputStream = inputStream;
    this.fileSize = fileSize;
  }

  @Override public int read() throws IOException {
    int c = inputStream.read();
    totalRead++;
    if (totalRead>fileSize) {
      return -1;
    }
    return c;
  }

  @Override public int read( byte[] bytes ) throws IOException {
    int readSize = inputStream.read( bytes );
    if (readSize>0) {
      totalRead+=readSize;
    }
    return readSize;
  }

  @Override public int read( byte[] bytes, int i, int i1 ) throws IOException {
    int readSize = inputStream.read( bytes, i, i1 );
    if (readSize>0) {
      // This blob is padded to a page boundary
      // Just return the remainder.  The rest are 0 anyway
      //
      if (totalRead+readSize>fileSize) {
        int actuallyRead = (int) (fileSize-totalRead);
        totalRead = fileSize;
        return actuallyRead;
      }
    }
    return readSize;
  }

  @Override public long skip( long l ) throws IOException {
    long skippedLength = inputStream.skip( l );
    if (skippedLength>0) {
      totalRead+=skippedLength;
      // This blob is padded to a page boundary
      // Just return the remainder.  The rest are 0 anyway
      //
      if (totalRead+skippedLength>fileSize) {
        int actuallyRead = (int) (fileSize-totalRead);
        totalRead = fileSize;
        return actuallyRead;
      }
    }
    return skippedLength;
  }

  @Override public int available() throws IOException {
    return (int) (fileSize-totalRead);
  }

  @Override public void close() throws IOException {
    inputStream.close();
  }

  @Override public synchronized void mark( int i ) {
    inputStream.mark( i );
  }

  @Override public synchronized void reset() throws IOException {
    inputStream.reset();
  }

  @Override public boolean markSupported() {
    return inputStream.markSupported();
  }
}
