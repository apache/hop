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

  public static final String MY_MIGRATION_STEP_NAME = "step_name";
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

  @MetaStoreAttribute( key = MY_MIGRATION_STEP_NAME )
  protected String stepName;

  @MetaStoreAttribute( key = MY_MIGRATION_HOST_NAME )
  protected String hostname;

  @MetaStoreAttribute( key = MY_MIGRATION_STEP_NAME )
  protected String stepname;

  @MetaStoreAttribute( key = MY_MIGRATION_FIELD_MAPPINGS )
  protected String fieldMappings;

  @MetaStoreAttribute( key = MY_MIGRATION_PARAMETER_NAME )
  protected String parameterName;

  @MetaStoreAttribute( key = MY_MIGRATION_SOURCE_FIELD_NAME )
  protected String sourceFieldName;

  @MetaStoreAttribute( key = MY_MIGRATION_TARGET_FIELD_NAME )
  protected String targetFieldName;

  public String getName() {
    return name;
  }

  public void setName( String name ) {
    this.name = name;
  }

  public String getStepName() {
    return stepName;
  }

  public void setStepName( String stepName ) {
    this.stepName = stepName;
  }

  public String getHostname() {
    return hostname;
  }

  public void setHostname( String hostname ) {
    this.hostname = hostname;
  }

  public String getStepname() {
    return stepname;
  }

  public void setStepname( String stepname ) {
    this.stepname = stepname;
  }

  public String getFieldMappings() {
    return fieldMappings;
  }

  public void setFieldMappings( String fieldMappings ) {
    this.fieldMappings = fieldMappings;
  }

  public String getParameterName() {
    return parameterName;
  }

  public void setParameterName( String parameterName ) {
    this.parameterName = parameterName;
  }

  public String getSourceFieldName() {
    return sourceFieldName;
  }

  public void setSourceFieldName( String sourceFieldName ) {
    this.sourceFieldName = sourceFieldName;
  }

  public String getTargetFieldName() {
    return targetFieldName;
  }

  public void setTargetFieldName( String targetFieldName ) {
    this.targetFieldName = targetFieldName;
  }
}
