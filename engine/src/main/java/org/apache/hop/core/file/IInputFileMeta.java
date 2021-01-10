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

package org.apache.hop.core.file;

import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;

/**
 * @deprecated replaced by implementation in the ...transforms.fileinput.text package
 */
@Deprecated
public interface IInputFileMeta<Main extends ITransform, Data extends ITransformData> extends ITransformMeta<Main, Data> {

  TextFileInputField[] getInputFields();

  int getFileFormatTypeNr();

  boolean hasHeader();

  int getNrHeaderLines();

  String[] getFilePaths( IVariables variables );

  boolean isErrorIgnored();

  String getErrorCountField();

  String getErrorFieldsField();

  String getErrorTextField();

  String getFileType();

  String getEnclosure();

  String getEscapeCharacter();

  String getSeparator();

  boolean isErrorLineSkipped();

  boolean includeFilename();

  boolean includeRowNumber();
}
