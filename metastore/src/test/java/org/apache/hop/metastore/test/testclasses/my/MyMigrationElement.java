/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

/*!
 * This program is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License, version 2.1 as published by the Free Software
 * Foundation.
 *
 * You should have received a copy of the GNU Lesser General Public License along with this
 * program; if not, you can obtain a copy at http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
 * or from the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Lesser General Public License for more details.
 *
 * Copyright (c) 2002-2017 Hitachi Vantara..  All rights reserved.
 */
package org.apache.hop.metastore.test.testclasses.my;

import org.apache.hop.metastore.persist.MetaStoreAttribute;
import org.apache.hop.metastore.persist.MetaStoreElementType;

@MetaStoreElementType(
  name = "My migration element type",
  description = "This is my migration element type" )
public class MyMigrationElement {

  public static final String MY_MIGRATION_PIPELINE_NAME = "pipeline_name";
  public static final String MY_MIGRATION_TRANSFORM_NAME = "transform_name";
  public static final String MY_MIGRATION_HOST_NAME = "host_name";
  public static final String MY_MIGRATION_FIELD_MAPPINGS = "field_mappings";
  public static final String MY_MIGRATION_PARAMETER_NAME = "parameter_name";
  public static final String MY_MIGRATION_SOURCE_FIELD_NAME = "source_field_name";
  public static final String MY_MIGRATION_TARGET_FIELD_NAME = "target_field_name";

  public MyMigrationElement() {
    // Required
  }

  public MyMigrationElement( String name ) {
    this.name = name;
  }

  protected String name;

  @MetaStoreAttribute( key = MY_MIGRATION_PIPELINE_NAME )
  protected String pipelineName;

  @MetaStoreAttribute( key = MY_MIGRATION_HOST_NAME )
  protected String hostname;

  @MetaStoreAttribute( key = MY_MIGRATION_TRANSFORM_NAME )
  protected String transformName;

  @MetaStoreAttribute( key = MY_MIGRATION_FIELD_MAPPINGS )
  protected String fieldMappings;

  @MetaStoreAttribute( key = MY_MIGRATION_PARAMETER_NAME )
  protected String parameterName;

  @MetaStoreAttribute( key = MY_MIGRATION_SOURCE_FIELD_NAME )
  protected String sourceFieldName;

  @MetaStoreAttribute( key = MY_MIGRATION_TARGET_FIELD_NAME )
  protected String targetFieldName;

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * Gets pipelineName
   *
   * @return value of pipelineName
   */
  public String getPipelineName() {
    return pipelineName;
  }

  /**
   * @param pipelineName The pipelineName to set
   */
  public void setPipelineName( String pipelineName ) {
    this.pipelineName = pipelineName;
  }

  /**
   * Gets hostname
   *
   * @return value of hostname
   */
  public String getHostname() {
    return hostname;
  }

  /**
   * @param hostname The hostname to set
   */
  public void setHostname( String hostname ) {
    this.hostname = hostname;
  }

  /**
   * Gets transformName
   *
   * @return value of transformName
   */
  public String getTransformName() {
    return transformName;
  }

  /**
   * @param transformName The transformName to set
   */
  public void setTransformName( String transformName ) {
    this.transformName = transformName;
  }

  /**
   * Gets fieldMappings
   *
   * @return value of fieldMappings
   */
  public String getFieldMappings() {
    return fieldMappings;
  }

  /**
   * @param fieldMappings The fieldMappings to set
   */
  public void setFieldMappings( String fieldMappings ) {
    this.fieldMappings = fieldMappings;
  }

  /**
   * Gets parameterName
   *
   * @return value of parameterName
   */
  public String getParameterName() {
    return parameterName;
  }

  /**
   * @param parameterName The parameterName to set
   */
  public void setParameterName( String parameterName ) {
    this.parameterName = parameterName;
  }

  /**
   * Gets sourceFieldName
   *
   * @return value of sourceFieldName
   */
  public String getSourceFieldName() {
    return sourceFieldName;
  }

  /**
   * @param sourceFieldName The sourceFieldName to set
   */
  public void setSourceFieldName( String sourceFieldName ) {
    this.sourceFieldName = sourceFieldName;
  }

  /**
   * Gets targetFieldName
   *
   * @return value of targetFieldName
   */
  public String getTargetFieldName() {
    return targetFieldName;
  }

  /**
   * @param targetFieldName The targetFieldName to set
   */
  public void setTargetFieldName( String targetFieldName ) {
    this.targetFieldName = targetFieldName;
  }
}
