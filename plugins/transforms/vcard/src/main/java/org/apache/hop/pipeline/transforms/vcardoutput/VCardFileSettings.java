/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.pipeline.transforms.vcardoutput;

import java.text.SimpleDateFormat;
import java.util.Date;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class VCardFileSettings {

  @HopMetadataProperty(key = "name")
  private String fileName;

  @HopMetadataProperty(key = "extension")
  private String extension;

  @HopMetadataProperty(key = "create_parent_folder")
  private boolean createParentFolder;

  @HopMetadataProperty(key = "append")
  private boolean append;

  @HopMetadataProperty(key = "add_date")
  private boolean dateInFileName;

  @HopMetadataProperty(key = "add_time")
  private boolean timeInFileName;

  @HopMetadataProperty(key = "doNotOpenNewFileInit")
  private boolean doNotOpenNewFileInit;

  public VCardFileSettings() {
    extension = "vcf";
    createParentFolder = true;
    doNotOpenNewFileInit = true;
  }

  public VCardFileSettings(VCardFileSettings other) {
    this();
    this.fileName = other.fileName;
    this.extension = other.extension;
    this.createParentFolder = other.createParentFolder;
    this.append = other.append;
    this.dateInFileName = other.dateInFileName;
    this.timeInFileName = other.timeInFileName;
    this.doNotOpenNewFileInit = other.doNotOpenNewFileInit;
  }

  /**
   * The file name as the dialog shows it.
   *
   * @param showSamples true for a sample with the current date and time, false for the name with
   *     {@code <yyyyMMdd>} and {@code <HHmmss>} where the date and time will go
   */
  public String buildFilename(IVariables variables, boolean showSamples) {
    if (showSamples) {
      return buildFilename(variables, new Date());
    }
    return buildFilename(variables, "<yyyyMMdd>", "<HHmmss>");
  }

  /**
   * The name of the file to write.
   *
   * @param date the moment that goes into the name when the date or the time is included
   */
  public String buildFilename(IVariables variables, Date date) {
    return buildFilename(
        variables,
        new SimpleDateFormat("yyyyMMdd").format(date),
        new SimpleDateFormat("HHmmss").format(date));
  }

  private String buildFilename(IVariables variables, String datePart, String timePart) {
    String filename = variables.resolve(fileName);
    String realExtension = variables.resolve(extension);

    if (dateInFileName) {
      filename += "_" + datePart;
    }
    if (timeInFileName) {
      filename += "_" + timePart;
    }
    if (!Utils.isEmpty(realExtension)) {
      filename += "." + realExtension;
    }
    return filename;
  }

  public String[] getFiles(IVariables variables, boolean showSamples) {
    if (showSamples) {
      return new String[] {buildFilename(variables, true), "..."};
    }
    return new String[] {buildFilename(variables, false)};
  }
}
