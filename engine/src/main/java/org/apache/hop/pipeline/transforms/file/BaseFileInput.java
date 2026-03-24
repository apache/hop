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

package org.apache.hop.pipeline.transforms.file;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.fileinput.InputFile;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.resource.ResourceEntry;
import org.apache.hop.resource.ResourceEntry.ResourceType;
import org.apache.hop.resource.ResourceReference;

/** Input files settings. */
@Getter
@Setter
public class BaseFileInput implements Cloneable {
  private static final Class<?> PKG = BaseFileInput.class;

  public static final String NO = "N";

  public static final String YES = "Y";

  protected static final String[] RequiredFilesCode = new String[] {"N", "Y"};
  protected static final String[] RequiredFilesDesc =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };

  @HopMetadataProperty(
      key = "file",
      groupKey = "files",
      injectionKey = "FILENAME_LINE",
      injectionKeyDescription = "TextFileInput.Injection.FILENAME_LINE",
      injectionGroupKey = "FILENAME_LINES",
      injectionGroupDescription = "TextFileInput.Injection.FILENAME_LINES")
  private List<InputFile> inputFiles;

  /** Are we accepting filenames in input rows? */
  @Injection(name = "ACCEPT_FILE_NAMES")
  @HopMetadataProperty(
      key = "accept_filenames",
      injectionKey = "ACCEPT_FILE_NAMES",
      injectionKeyDescription = "TextFileInput.Injection.ACCEPT_FILE_NAMES")
  private boolean acceptingFilenames;

  /** The transformName to accept filenames from */
  @Injection(name = "ACCEPT_FILE_TRANSFORM")
  @HopMetadataProperty(
      key = "accept_transform_name",
      injectionKey = "ACCEPT_FILE_TRANSFORM",
      injectionKeyDescription = "TextFileInput.Injection.ACCEPT_FILE_TRANSFORM")
  private String acceptingTransformName;

  /** If receiving input rows, should we pass through existing fields? */
  @Injection(name = "PASS_THROUGH_FIELDS")
  @HopMetadataProperty(
      key = "passing_through_fields",
      injectionKey = "PASS_THROUGH_FIELDS",
      injectionKeyDescription = "TextFileInput.Injection.PASS_THROUGH_FIELDS")
  private boolean passingThruFields;

  /** The field in which the filename is placed */
  @Injection(name = "ACCEPT_FILE_FIELD")
  @HopMetadataProperty(
      key = "accept_field",
      injectionKey = "ACCEPT_FILE_FIELD",
      injectionKeyDescription = "TextFileInput.Injection.ACCEPT_FILE_FIELD")
  private String acceptingField;

  /** The add filenames to result filenames flag */
  @HopMetadataProperty(
      key = "add_to_result_filenames",
      injectionKey = "ADD_FILES_TO_RESULT",
      injectionKeyDescription = "TextFileInput.Injection.ADD_FILES_TO_RESULT")
  private boolean addingResult;

  public BaseFileInput() {
    inputFiles = new ArrayList<>();
  }

  public BaseFileInput(BaseFileInput b) {
    this();
    this.acceptingField = b.acceptingField;
    this.acceptingFilenames = b.acceptingFilenames;
    this.acceptingTransformName = b.acceptingTransformName;
    this.addingResult = b.addingResult;
    this.passingThruFields = b.passingThruFields;
    b.inputFiles.forEach(f -> inputFiles.add(new InputFile(f)));
  }

  @Override
  public Object clone() {
    return new BaseFileInput(this);
  }

  public static String getRequiredFilesCode(String tt) {
    if (tt == null) {
      return RequiredFilesCode[0];
    }
    if (tt.equals(RequiredFilesDesc[1])) {
      return RequiredFilesCode[1];
    } else {
      return RequiredFilesCode[0];
    }
  }

  public List<ResourceReference> getResourceDependencies(
      IVariables variables, TransformMeta transformMeta) {
    List<ResourceReference> references = new ArrayList<>(5);
    ResourceReference reference = new ResourceReference(transformMeta);
    references.add(reference);

    String[] textFiles = FileInputList.createFilePathList(variables, inputFiles);
    for (String textFile : textFiles) {
      reference.getEntries().add(new ResourceEntry(textFile, ResourceType.FILE));
    }

    return references;
  }
}
