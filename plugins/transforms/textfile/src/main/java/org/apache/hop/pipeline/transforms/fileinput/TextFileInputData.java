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

package org.apache.hop.pipeline.transforms.fileinput;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.compress.CompressionInputStream;
import org.apache.hop.core.file.EncodingType;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.playlist.IFilePlayList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.errorhandling.IFileErrorHandler;
import org.apache.hop.pipeline.transforms.csvinput.ICrLfMatcher;
import org.apache.hop.pipeline.transforms.csvinput.SingleByteCrLfMatcher;

/**
 * @deprecated replaced by implementation in the ...transforms.fileinput.text package
 */
@Deprecated(since = "2.0")
@SuppressWarnings("java:S1104")
public class TextFileInputData extends BaseTransformData implements ITransformData {

  public FileChannel fc;

  public List<TextFileLine> lineBuffer;

  public ICrLfMatcher crLfMatcher;

  public ByteBuffer bb;

  public byte[] byteBuffer;

  public int endBuffer;

  private int startBuffer;

  public int preferredBufferSize;

  private int bufferSize;

  public Object[] previousRow;

  public int nr_repeats;

  public int nrLinesOnPage;

  private FileInputList files;

  public HashMap<FileObject, Object[]> passThruFields;

  public Object[] currentPassThruFieldsRow;

  public int nrPassThruFields;

  public boolean isLastFile;

  public String filename;

  public int lineInFile;

  public FileObject file;

  public int filenr;

  public CompressionInputStream in;

  public InputStreamReader isr;

  public boolean doneReading;

  public int headerLinesRead;

  public int footerLinesRead;

  public int pageLinesRead;

  public boolean doneWithHeader;

  public IFileErrorHandler dataErrorLineHandler;

  public IFilePlayList filePlayList;

  public TextFileFilterProcessor filterProcessor;

  public IRowMeta outputRowMeta;

  public StringBuilder lineStringBuilder;

  public int fileFormatType;

  public int fileType;

  public IRowMeta convertRowMeta;

  public IRowSet rowSet;

  /** The separator (delimiter) */
  public String separator;

  public String enclosure;

  public String escapeCharacter;

  public boolean addShortFilename;
  public boolean addExtension;
  public boolean addPath;
  public boolean addSize;
  public boolean addIsHidden;
  public boolean addLastModificationDate;
  public boolean addUri;
  public boolean addRootUri;

  public long totalBytesRead;
  public String shortFilename;
  public String path;
  public String extension;
  public boolean hidden;
  public Date lastModificationDateTime;
  public String uriName;
  public String rootUriName;
  public long size;

  public EncodingType encodingType;

  public Map<String, Boolean> rejectedFiles;

  public TextFileInputData() {
    super();

    // linked list is better, as usually .remove(0) is applied to this list
    lineBuffer = new LinkedList<>();

    nr_repeats = 0;
    previousRow = null;
    filenr = 0;

    nrLinesOnPage = 0;

    in = null;

    filterProcessor = null;
    lineStringBuilder = new StringBuilder(256);

    rejectedFiles = new HashMap<>();

    crLfMatcher = new SingleByteCrLfMatcher();
    byteBuffer = new byte[] {};
    endBuffer = 0;
    totalBytesRead = 0;
    preferredBufferSize = 0;
  }

  public FileInputList getFiles() {
    return files;
  }

  public void setFiles(FileInputList files) {
    this.files = files;
  }

  public boolean newLineFound() {
    return crLfMatcher.isReturn(byteBuffer, endBuffer)
        || crLfMatcher.isLineFeed(byteBuffer, endBuffer);
  }

  boolean moveEndBufferPointer() throws IOException {
    return moveEndBufferPointer(true);
  }

  /**
   * This method should be used very carefully. Moving pointer without increasing number of written
   * bytes can lead to data corruption.
   */
  boolean moveEndBufferPointer(boolean increaseTotalBytes) throws IOException {
    endBuffer++;
    if (increaseTotalBytes) {
      totalBytesRead++;
    }
    return resizeBufferIfNeeded();
  }

  private void resizeByteBufferArray() {
    // What's the new size?
    // It's (endBuffer-startBuffer)+size !!
    // That way we can at least read one full block of data using NIO
    //
    bufferSize = endBuffer - startBuffer;
    int newSize = bufferSize + preferredBufferSize;
    byte[] newByteBuffer = new byte[newSize + 100];

    // copy over the old data...
    System.arraycopy(byteBuffer, startBuffer, newByteBuffer, 0, bufferSize);

    // replace the old byte buffer...
    byteBuffer = newByteBuffer;

    // Adjust start and end point of data in the byte buffer
    //
    startBuffer = 0;
    endBuffer = bufferSize;
  }

  boolean resizeBufferIfNeeded() throws IOException {
    if (endOfBuffer()) {
      // Oops, we need to read more data...
      // Better resize this before we read other things in it...
      //
      resizeByteBufferArray();

      // Also read another chunk of data, now that we have the variables for it...
      //
      int n = readBufferFromFile();

      // If we didn't manage to read something, we return true to indicate we're done
      //
      return n < 0;
    }

    return false;
  }

  private int readBufferFromFile() throws IOException {
    // See if the line is not longer than the buffer.
    // In that case we need to increase the size of the byte buffer.
    // Since this method doesn't get called every other character, I'm sure we can spend a bit of
    // time here without
    // major performance loss.
    //
    if (endBuffer >= bb.capacity()) {
      resizeByteBuffer((int) (bb.capacity() * 1.5));
    }

    bb.position(endBuffer);
    int n = fc.read(bb);
    if (n >= 0) {

      // adjust the highest used position...
      //
      bufferSize = endBuffer + n;

      // Make sure we have room in the target byte buffer array
      //
      if (byteBuffer.length < bufferSize) {
        byte[] newByteBuffer = new byte[bufferSize];
        System.arraycopy(byteBuffer, 0, newByteBuffer, 0, byteBuffer.length);
        byteBuffer = newByteBuffer;
      }

      // Store the data in our byte array
      //
      bb.position(endBuffer);
      bb.get(byteBuffer, endBuffer, n);
    }

    return n;
  }

  private void resizeByteBuffer(int newSize) {
    ByteBuffer newBuffer = ByteBuffer.allocateDirect(newSize); // Increase by 50%
    newBuffer.position(0);
    newBuffer.put(bb);
    bb = newBuffer;
  }

  boolean endOfBuffer() {
    return endBuffer >= bufferSize;
  }
}
