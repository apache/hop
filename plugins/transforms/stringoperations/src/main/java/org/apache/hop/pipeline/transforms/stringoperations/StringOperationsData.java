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

package org.apache.hop.pipeline.transforms.stringoperations;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

/**
 * Apply certain operations too string.
 *
 * @author Samatar Hassan
 * @since 02 April 2009
 */
public class StringOperationsData extends BaseTransformData implements ITransformData {

  public int[] inStreamNrs; // string infields

  public String[] outStreamNrs;

  /**
   * Runtime trim operators
   */
  public int[] trimOperators;

  /**
   * Runtime trim operators
   */
  public int[] lowerUpperOperators;

  public int[] padType;

  public String[] padChar;

  public int[] padLen;

  public int[] initCap;

  public int[] maskHTML;

  public int[] digits;

  public int[] removeSpecialCharacters;

  public IRowMeta outputRowMeta;

  public int inputFieldsNr;

  public int nrFieldsInStream;

  /**
   * Default constructor.
   */
  public StringOperationsData() {
    super();
    this.inputFieldsNr = 0;
    this.nrFieldsInStream = 0;
  }
}
