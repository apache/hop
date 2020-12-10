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

package org.apache.hop.pipeline.transforms.common;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.pipeline.PipelineMeta;

/**
 * A common interface for all metas aware of the csv input format, such as CSV Input and Text File Input
 */
public interface ICsvInputAwareMeta {

  String getDelimiter();

  String getEncoding();

  String getEnclosure();

  String getEscapeCharacter();

  int getFileFormatTypeNr();

  boolean hasHeader();

  /**
   * Returns a {@link FileObject} that corresponds to the first encountered input file. This object is used to read the
   * file headers for the purpose of field parsing.
   *
   * @param variables the {@link PipelineMeta}
   * @return null if the {@link FileObject} cannot be created.
   */
  FileObject getHeaderFileObject( final IVariables variables );

}
