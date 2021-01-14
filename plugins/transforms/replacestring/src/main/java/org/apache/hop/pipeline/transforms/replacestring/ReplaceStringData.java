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

package org.apache.hop.pipeline.transforms.replacestring;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

import java.util.regex.Pattern;

/**
 * @author Samatar Hassan
 * @since 28 September 2008
 */
public class ReplaceStringData extends BaseTransformData implements ITransformData {

  public int[] inStreamNrs;

  public String[] outStreamNrs;

  public int[] useRegEx;

  public String[] replaceString;

  public String[] replaceByString;

  public boolean[] setEmptyString;

  public int[] replaceFieldIndex;

  public int[] wholeWord;

  public int[] caseSensitive;

  public int[] isUnicode;

  public String realChangeField;

  public String[] valueChange;

  public String finalvalueChange;

  public IRowMeta outputRowMeta;

  public int inputFieldsNr;

  public Pattern[] patterns;

  public int numFields;

  /**
   * Default constructor.
   */
  public ReplaceStringData() {
    super();
    realChangeField = null;
    valueChange = null;
    finalvalueChange = null;
    inputFieldsNr = 0;
  }
}
