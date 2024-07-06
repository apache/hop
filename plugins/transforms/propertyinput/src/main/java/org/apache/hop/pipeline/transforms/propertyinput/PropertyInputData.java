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

package org.apache.hop.pipeline.transforms.propertyinput;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.text.DateFormatSymbols;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;
import org.ini4j.Profile.Section;
import org.ini4j.Wini;

@SuppressWarnings("java:S1104")
public class PropertyInputData extends BaseTransformData implements ITransformData {
  public String currentLine;
  public IRowMeta outputRowMeta;
  public IRowMeta convertRowMeta;
  public Object[] previousRow;
  public int repeatsCount;

  public NumberFormat numberFormat;
  public DecimalFormat decimalFormat;
  public DecimalFormatSymbols decimalFormatSymbols;
  public SimpleDateFormat simpleDateFormat;
  public DateFormatSymbols dateFormatSymbols;

  public FileInputList files;
  public boolean lastFile;
  public FileObject file;
  public int fileNumber;

  public FileInputStream fr;
  public BufferedInputStream is;
  public long rowNumber;
  public Map<String, Object> rw;
  public IRowMeta inputRowMeta;
  public int totalPreviousFields;
  public int indexOfFilenameField;
  public Object[] inputRow;

  // Properties files
  public Properties properties;
  public Iterator<Object> iterator;

  // INI files
  public Section iniSection;
  public Wini wini;
  public Iterator<String> sectionNameIterator;
  public String realEncoding;
  public String realSection;
  public Iterator<String> iniNameIterator;

  public boolean propFiles;

  public String filename;
  public String shortFilename;
  public String path;
  public String extension;
  public boolean hidden;
  public Date lastModificationDateTime;
  public String uriName;
  public String rootUriName;
  public long size;

  public PropertyInputData() {
    super();
    previousRow = null;
    currentLine = null;
    numberFormat = NumberFormat.getInstance();
    decimalFormat = (DecimalFormat) numberFormat;
    decimalFormatSymbols = new DecimalFormatSymbols();
    simpleDateFormat = new SimpleDateFormat();
    dateFormatSymbols = new DateFormatSymbols();

    repeatsCount = 0;
    previousRow = null;
    fileNumber = 0;

    fr = null;
    is = null;
    rw = null;
    totalPreviousFields = 0;
    indexOfFilenameField = -1;
    inputRow = null;

    properties = null;
    iterator = null;
    iniSection = null;
    wini = null;
    sectionNameIterator = null;
    realEncoding = null;
    realSection = null;
    propFiles = true;
    iniNameIterator = null;
  }
}
