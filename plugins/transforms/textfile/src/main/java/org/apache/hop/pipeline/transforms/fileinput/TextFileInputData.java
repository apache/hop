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

import java.io.InputStreamReader;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Matt
 * @since 22-jan-2005
 * @deprecated replaced by implementation in the ...transforms.fileinput.text package
 */
@Deprecated
public class TextFileInputData extends BaseTransformData implements ITransformData {

  public List<TextFileLine> lineBuffer;

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

  /**
   * The separator (delimiter)
   */
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
    lineStringBuilder = new StringBuilder( 256 );

    rejectedFiles = new HashMap<>();
  }

  public FileInputList getFiles() {
    return files;
  }

  public void setFiles( FileInputList files ) {
    this.files = files;
  }
}
