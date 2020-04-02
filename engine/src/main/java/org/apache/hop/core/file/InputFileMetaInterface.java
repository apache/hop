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

package org.apache.hop.core.file;

import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.pipeline.transform.TransformDataInterface;
import org.apache.hop.pipeline.transform.TransformInterface;
import org.apache.hop.pipeline.transform.TransformMetaInterface;

/**
 * @deprecated replaced by implementation in the ...transforms.fileinput.text package
 */
@Deprecated
public interface InputFileMetaInterface<Main extends TransformInterface, Data extends TransformDataInterface> extends TransformMetaInterface<Main, Data> {

  TextFileInputField[] getInputFields();

  int getFileFormatTypeNr();

  boolean hasHeader();

  int getNrHeaderLines();

  String[] getFilePaths( VariableSpace space );

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
