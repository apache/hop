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

package org.apache.hop.pipeline.transforms.fileinput.text;

import org.apache.hop.core.file.EncodingType;
import org.apache.hop.core.playlist.IFilePlayList;
import org.apache.hop.pipeline.transforms.file.BaseFileInputTransformData;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Matt
 * @since 22-jan-2005
 */
public class TextFileInputData extends BaseFileInputTransformData {

  public List<TextFileLine> lineBuffer;

  public Object[] previousRow;

  public int nrLinesOnPage;

  public boolean doneReading;

  public int headerLinesRead;

  public int footerLinesRead;

  public int pageLinesRead;

  public boolean doneWithHeader;

  public IFilePlayList filePlayList;

  public TextFileFilterProcessor filterProcessor;

  public StringBuilder lineStringBuilder;

  public int fileFormatType;

  public int fileType;

  /**
   * The separator (delimiter)
   */
  public String separator;

  public String enclosure;

  public String escapeCharacter;

  public EncodingType encodingType;

  public TextFileInputData() {
    // linked list is better, as usually .remove(0) is applied to this list
    lineBuffer = new LinkedList<>();

    nr_repeats = 0;
    previousRow = null;

    nrLinesOnPage = 0;

    filterProcessor = null;
    lineStringBuilder = new StringBuilder( 256 );
  }
}
