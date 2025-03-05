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
package org.apache.hop.pipeline.transforms.accessoutput;

import io.github.spannm.jackcess.Database.FileFormat;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

public enum AccessFileFormat implements IEnumHasCodeAndDescription {
  V2003(FileFormat.V2003, "MS Access 2002/2003"),
  V2007(FileFormat.V2007, "MS Access 2007"),
  V2010(FileFormat.V2010, "MS Access 2010+"),
  V2016(FileFormat.V2016, "MS Access 2016+"),
  V2019(FileFormat.V2019, "MS Access 2019+ (Office 365)");

  private final FileFormat format;

  private final String code;
  private final String description;

  AccessFileFormat(FileFormat format, String description) {
    this.format = format;
    this.code = format.name();
    this.description = description;
  }

  /**
   * Return an array of file format descriptions
   *
   * @return An array of file format descriptions
   */
  public static String[] getDescriptions() {
    return IEnumHasCodeAndDescription.getDescriptions(AccessFileFormat.class);
  }

  public static AccessFileFormat lookupDescription(String description) {
    return IEnumHasCodeAndDescription.lookupDescription(AccessFileFormat.class, description, V2019);
  }

  /**
   * Gets code
   *
   * @return value of code
   */
  @Override
  public String getCode() {
    return code;
  }

  /**
   * Gets description
   *
   * @return value of description
   */
  @Override
  public String getDescription() {
    return description;
  }

  public FileFormat getFormat() {
    return format;
  }
}
