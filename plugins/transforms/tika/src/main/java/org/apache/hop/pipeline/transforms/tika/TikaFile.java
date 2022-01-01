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

package org.apache.hop.pipeline.transforms.tika;

import org.apache.hop.metadata.api.HopMetadataProperty;

public class TikaFile {
  /** The name of the file or folder */
  @HopMetadataProperty(key = "name", injectionKeyDescription = "TikaFile.Injection.Name")
  private String name;

  /** Wildcard or file mask (regular expression) */
  @HopMetadataProperty(key = "mask", injectionKeyDescription = "TikaFile.Injection.Mask")
  private String mask;

  /** Wildcard or file mask to exclude (regular expression) */
  @HopMetadataProperty(
      key = "exclude-mask",
      injectionKeyDescription = "TikaFile.Injection.ExcludeMask")
  private String excludeMask;

  /** boolean value indicating if a file is required to be present. */
  @HopMetadataProperty(key = "required", injectionKeyDescription = "TikaFile.Injection.Required")
  private boolean required;

  /** boolean value indicating if we need to fetch/search sub folders. */
  @HopMetadataProperty(
      key = "include-sub-folders",
      injectionKeyDescription = "TikaFile.Injection.IncludeSubFolders")
  private boolean includingSubFolders;

  public TikaFile() {}

  public TikaFile(
      String name, String mask, String excludeMask, boolean required, boolean includingSubFolders) {
    this.name = name;
    this.mask = mask;
    this.excludeMask = excludeMask;
    this.required = required;
    this.includingSubFolders = includingSubFolders;
  }

  public TikaFile(TikaFile f) {
    this.name = f.name;
    this.mask = f.mask;
    this.excludeMask = f.excludeMask;
    this.required = f.required;
    this.includingSubFolders = f.includingSubFolders;
  }

  @Override
  public TikaFile clone() throws CloneNotSupportedException {
    return new TikaFile(this);
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /** @param name The name to set */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Gets mask
   *
   * @return value of mask
   */
  public String getMask() {
    return mask;
  }

  /** @param mask The mask to set */
  public void setMask(String mask) {
    this.mask = mask;
  }

  /**
   * Gets excludeMask
   *
   * @return value of excludeMask
   */
  public String getExcludeMask() {
    return excludeMask;
  }

  /** @param excludeMask The excludeMask to set */
  public void setExcludeMask(String excludeMask) {
    this.excludeMask = excludeMask;
  }

  /**
   * Gets required
   *
   * @return value of required
   */
  public boolean isRequired() {
    return required;
  }

  /** @param required The required to set */
  public void setRequired(boolean required) {
    this.required = required;
  }

  /**
   * Gets includingSubFolders
   *
   * @return value of includingSubFolders
   */
  public boolean isIncludingSubFolders() {
    return includingSubFolders;
  }

  /** @param includingSubFolders The includingSubFolders to set */
  public void setIncludingSubFolders(boolean includingSubFolders) {
    this.includingSubFolders = includingSubFolders;
  }
}
