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

package org.apache.hop.pipeline.transforms.getfilenames;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.playlist.IFilePlayList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.errorhandling.IFileErrorHandler;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.DateFormatSymbols;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipInputStream;

/**
 * @author Matt
 * @since 22-jan-2005
 */
public class GetFileNamesData extends BaseTransformData implements ITransformData {
  public List<String> lineBuffer;

  public Object[] previousRow;

  public int nr_repeats;

  public int nrLinesOnPage;

  public NumberFormat nf;

  public DecimalFormat df;

  public DecimalFormatSymbols dfs;

  public SimpleDateFormat daf;

  public IRowMeta outputRowMeta;

  public DateFormatSymbols dafs;

  public FileInputList files;

  public boolean isLastFile;

  public String filename;

  public int filenr;

  public int filessize;

  public FileInputStream fr;

  public ZipInputStream zi;

  public InputStreamReader isr;

  public boolean doneReading;

  public int headerLinesRead;

  public int footerLinesRead;

  public int pageLinesRead;

  public boolean doneWithHeader;

  public IFileErrorHandler dataErrorLineHandler;

  public IFilePlayList filePlayList;

  public FileObject file;

  public long rownr;

  public int totalpreviousfields;

  public int indexOfFilenameField;

  public int indexOfWildcardField;
  public int indexOfExcludeWildcardField;

  public IRowMeta inputRowMeta;

  public Object[] readrow;

  public int nrTransformFields;

  public GetFileNamesData() {
    super();

    lineBuffer = new ArrayList<>();
    nf = NumberFormat.getInstance();
    df = (DecimalFormat) nf;
    dfs = new DecimalFormatSymbols();
    daf = new SimpleDateFormat();
    dafs = new DateFormatSymbols();

    nr_repeats = 0;
    previousRow = null;
    filenr = 0;
    filessize = 0;

    nrLinesOnPage = 0;

    fr = null;
    zi = null;
    file = null;
    totalpreviousfields = 0;
    indexOfFilenameField = -1;
    indexOfWildcardField = -1;
    readrow = null;
    nrTransformFields = 0;
    indexOfExcludeWildcardField = -1;
  }

  public void setDateFormatLenient( boolean lenient ) {
    daf.setLenient( lenient );
  }

}
