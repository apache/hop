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
 *
 */

package org.apache.hop.pipeline;

import java.util.Date;
import org.apache.hop.metadata.api.HopMetadataProperty;

public abstract class AbstractMetaInfo {

  @HopMetadataProperty protected String name;

  @HopMetadataProperty(key = "name_sync_with_filename")
  protected boolean nameSynchronizedWithFilename;

  @HopMetadataProperty(groupKey = "info", key = "description")
  protected String description;

  @HopMetadataProperty(groupKey = "info", key = "extended_description")
  protected String extendedDescription;

  @HopMetadataProperty(key = "created_user")
  protected String createdUser;

  @HopMetadataProperty(key = "modified_user")
  protected String modifiedUser;

  @HopMetadataProperty(key = "created_date")
  protected Date createdDate;

  @HopMetadataProperty(key = "modified_date")
  protected Date modifiedDate;

  protected AbstractMetaInfo() {
    this.nameSynchronizedWithFilename = true;
    this.createdDate = new Date();
    this.modifiedDate = new Date();
    this.createdUser = "-";
    this.modifiedUser = "-";
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * Sets name
   *
   * @param name value of name
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Gets nameSynchronizedWithFilename
   *
   * @return value of nameSynchronizedWithFilename
   */
  public boolean isNameSynchronizedWithFilename() {
    return nameSynchronizedWithFilename;
  }

  /**
   * Sets nameSynchronizedWithFilename
   *
   * @param nameSynchronizedWithFilename value of nameSynchronizedWithFilename
   */
  public void setNameSynchronizedWithFilename(boolean nameSynchronizedWithFilename) {
    this.nameSynchronizedWithFilename = nameSynchronizedWithFilename;
  }

  /**
   * Gets description
   *
   * @return value of description
   */
  public String getDescription() {
    return description;
  }

  /**
   * Sets description
   *
   * @param description value of description
   */
  public void setDescription(String description) {
    this.description = description;
  }

  /**
   * Gets extendedDescription
   *
   * @return value of extendedDescription
   */
  public String getExtendedDescription() {
    return extendedDescription;
  }

  /**
   * Sets extendedDescription
   *
   * @param extendedDescription value of extendedDescription
   */
  public void setExtendedDescription(String extendedDescription) {
    this.extendedDescription = extendedDescription;
  }

  /**
   * Gets createdUser
   *
   * @return value of createdUser
   */
  public String getCreatedUser() {
    return createdUser;
  }

  /**
   * Sets createdUser
   *
   * @param createdUser value of createdUser
   */
  public void setCreatedUser(String createdUser) {
    this.createdUser = createdUser;
  }

  /**
   * Gets modifiedUser
   *
   * @return value of modifiedUser
   */
  public String getModifiedUser() {
    return modifiedUser;
  }

  /**
   * Sets modifiedUser
   *
   * @param modifiedUser value of modifiedUser
   */
  public void setModifiedUser(String modifiedUser) {
    this.modifiedUser = modifiedUser;
  }

  /**
   * Gets createdDate
   *
   * @return value of createdDate
   */
  public Date getCreatedDate() {
    return createdDate;
  }

  /**
   * Sets createdDate
   *
   * @param createdDate value of createdDate
   */
  public void setCreatedDate(Date createdDate) {
    this.createdDate = createdDate;
  }

  /**
   * Gets modifiedDate
   *
   * @return value of modifiedDate
   */
  public Date getModifiedDate() {
    return modifiedDate;
  }

  /**
   * Sets modifiedDate
   *
   * @param modifiedDate value of modifiedDate
   */
  public void setModifiedDate(Date modifiedDate) {
    this.modifiedDate = modifiedDate;
  }
}
