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

import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.fileinput.InputFile;
import org.apache.hop.core.gui.ITextFileInputField;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.file.BaseFileErrorHandling;

/**
 * A common interface for all metas aware of the csv input format, such as CSV Input and Text File
 * Input
 */
public interface ICsvInputAwareMeta<T extends ITextFileInputField> {

  String getFileType();

  List<T> getInputFields();

  List<InputFile> getInputFiles();

  String getDelimiter();

  String getEncoding();

  String getEnclosure();

  String getEscapeCharacter();

  boolean isBreakInEnclosureAllowed();

  int getFileFormatTypeNr();

  boolean hasHeader();

  int getNrHeaderLines();

  ICsvInputAwareMeta clone();

  /**
   * Returns a {@link FileObject} that corresponds to the first encountered input file. This object
   * is used to read the file headers for the purpose of field parsing.
   *
   * @param variables the {@link PipelineMeta}
   * @return null if the {@link FileObject} cannot be created.
   */
  FileObject getHeaderFileObject(final IVariables variables) throws HopFileException;

  /**
   * Gets the fields.
   *
   * @param inputRowMeta the input row meta that is modified in this method to reflect the output
   *     row metadata of the transform
   * @param name Name of the transform to use as input for the origin field in the values
   * @param info Fields used as extra lookup information
   * @param nextTransform the next transform that is targeted
   * @param variables the variables The variable variables to use to replace variables
   * @param metadataProvider the MetaStore to use to load additional external data or metadata
   *     impacting the output fields
   * @throws HopTransformException the hop transform exception
   */
  void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException;

  /**
   * @return Optional error handling metadata
   */
  BaseFileErrorHandling getErrorHandling();

  String getErrorCountField();

  String getErrorFieldsField();

  String getErrorTextField();

  boolean isErrorLineSkipped();

  boolean isIncludeFilename();

  boolean isIncludeRowNumber();

  String getLength();
}
