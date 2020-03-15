/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.apache.hop.trans.steps.fileinput;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.RowSet;
import org.apache.hop.core.compress.CompressionInputStream;
import org.apache.hop.core.file.EncodingType;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.playlist.FilePlayList;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.trans.step.BaseStepData;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.errorhandling.FileErrorHandler;

import java.io.InputStreamReader;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Matt
 * @since 22-jan-2005
 * @deprecated replaced by implementation in the ...steps.fileinput.text package
 */
@Deprecated
public class TextFileInputData extends BaseStepData implements StepDataInterface {

  public List<TextFileLine> lineBuffer;

  public Object[] previous_row;

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

  public FileErrorHandler dataErrorLineHandler;

  public FilePlayList filePlayList;

  public TextFileFilterProcessor filterProcessor;

  public RowMetaInterface outputRowMeta;

  public StringBuilder lineStringBuilder;

  public int fileFormatType;

  public int fileType;

  public RowMetaInterface convertRowMeta;

  public RowSet rowSet;

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
    lineBuffer = new LinkedList<TextFileLine>();

    nr_repeats = 0;
    previous_row = null;
    filenr = 0;

    nrLinesOnPage = 0;

    in = null;

    filterProcessor = null;
    lineStringBuilder = new StringBuilder( 256 );

    rejectedFiles = new HashMap<String, Boolean>();
  }

  public FileInputList getFiles() {
    return files;
  }

  public void setFiles( FileInputList files ) {
    this.files = files;
  }
}
