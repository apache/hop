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

package org.apache.hop.pipeline.transforms.tokenreplacement;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.io.OutputStream;
import java.text.DateFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class TokenReplacementData extends BaseTransformData implements ITransformData {
  public int splitnr;

  public int[] fieldnrs;

  public SimpleDateFormat daf;
  public DateFormatSymbols dafs;

  public List<String> openFiles;
  public List<OutputStream> openWriters;
  public List<OutputStream> openBufferedWriters;

  public SimpleDateFormat defaultDateFormat;
  public DateFormatSymbols defaultDateFormatSymbols;

  public IRowMeta outputRowMeta;
  public IRowMeta inputRowMeta;

  public int rowNumber;

  public TokenReplacementData() {
    super();

     daf = new SimpleDateFormat();
    dafs = new DateFormatSymbols();

    defaultDateFormat = new SimpleDateFormat();
    defaultDateFormatSymbols = new DateFormatSymbols();

    openFiles = new ArrayList<String>();
    openWriters = new ArrayList<OutputStream>();
    openBufferedWriters = new ArrayList<OutputStream>();

    rowNumber = 0;

    splitnr = 0;
  }
}
