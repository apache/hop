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

package org.apache.hop.pipeline.transforms.jsoninput.reader;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.transforms.jsoninput.JsonInputField;

import java.io.InputStream;

public interface IJsonReader {

  /**
   * Store and compile fields.
   * 
   * @param fields
   * @throws HopException
   */
  void setFields( JsonInputField[] fields ) throws HopException;

  boolean isIgnoreMissingPath();

  void setIgnoreMissingPath( boolean value );

  /**
   * parse compiled fields into a rowset
   */
  public IRowSet parse( InputStream in ) throws HopException;

}
