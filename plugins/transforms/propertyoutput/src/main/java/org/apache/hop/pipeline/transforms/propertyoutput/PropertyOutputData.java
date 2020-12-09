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

package org.apache.hop.pipeline.transforms.propertyoutput;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Properties;

/**
 * Output rows to Properties file and create a file.
 *
 * @author Samatar
 * @since 13-Apr-2008
 */

public class PropertyOutputData extends BaseTransformData implements ITransformData {
  public IRowMeta inputRowMeta;
  public IRowMeta outputRowMeta;
  private static final String DATE_FORMAT = "yyyy-MM-dd H:mm:ss";
  DateFormat dateParser;

  public int indexOfKeyField;
  public int indexOfValueField;

  public int indexOfFieldfilename;
  public HashSet<String> KeySet;
  public FileObject file;
  public String filename;

  public Properties pro;

  public String previousFileName;

  public PropertyOutputData() {
    super();

    dateParser = new SimpleDateFormat( DATE_FORMAT );

    indexOfKeyField = -1;
    indexOfValueField = -1;

    indexOfFieldfilename = -1;
    file = null;
    previousFileName = "";
    KeySet = new HashSet<>();
    filename = null;
  }

}
