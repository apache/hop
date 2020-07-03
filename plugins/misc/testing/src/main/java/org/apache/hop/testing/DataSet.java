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

package org.apache.hop.testing;


import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadata;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

@HopMetadata(
  key = "dataset",
  name = "Data Set",
  description = "This defines a data set, a static pre-defined collection of rows",
  iconImage = "dataset.svg"
)
public class DataSet extends Variables implements Cloneable, IVariables, IHopMetadata {

  public static final String VARIABLE_HOP_DATASETS_FOLDER = "HOP_DATASETS_FOLDER";

  @HopMetadataProperty
  private String name;

  @HopMetadataProperty
  private String description;

  @HopMetadataProperty( key = "folder_name" )
  private String folderName;

  @HopMetadataProperty( key = "base_filename" )
  private String baseFilename;

  @HopMetadataProperty( key = "dataset_fields" )
  private List<DataSetField> fields;

  public DataSet() {
    fields = new ArrayList<>();
  }

  public DataSet( String name, String description, String folderName, String baseFilename, List<DataSetField> fields ) {
    this();
    this.name = name;
    this.description = description;
    this.folderName = folderName;
    this.baseFilename = baseFilename;
    this.fields = fields;
  }

  @Override
  public boolean equals( Object obj ) {
    if ( this == obj ) {
      return true;
    }
    if ( !( obj instanceof DataSet ) ) {
      return false;
    }
    DataSet cmp = (DataSet) obj;
    return name.equals( cmp );
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }


  /**
   * Get standard Hop row metadata from the defined data set fields
   *
   * @return The row metadata
   * @throws HopPluginException
   */
  public IRowMeta getSetRowMeta() throws HopPluginException {
    IRowMeta rowMeta = new RowMeta();
    for ( DataSetField field : getFields() ) {
      IValueMeta valueMeta = ValueMetaFactory.createValueMeta(
        field.getFieldName(),
        field.getType(),
        field.getLength(),
        field.getPrecision()
      );
      valueMeta.setComments( field.getComment() );
      valueMeta.setConversionMask( field.getFormat() );
      rowMeta.addValueMeta( valueMeta );
    }
    return rowMeta;
  }

  public DataSetField findFieldWithName( String fieldName ) {
    for ( DataSetField field : fields ) {
      if ( field.getFieldName().equalsIgnoreCase( fieldName ) ) {
        return field;
      }
    }
    return null;
  }

  public int indexOfField( String fieldName ) {
    for ( int i = 0; i < fields.size(); i++ ) {
      DataSetField field = fields.get( i );
      if ( field.getFieldName().equalsIgnoreCase( fieldName ) ) {
        return i;
      }
    }
    return -1;
  }


  public List<Object[]> getAllRows( ILogChannel log, PipelineUnitTestSetLocation location ) throws HopException {
    return DataSetCsvUtil.getAllRows( log, this, location );
  }

  public List<Object[]> getAllRows( ILogChannel log ) throws HopException {
    return DataSetCsvUtil.getAllRows( this );
  }


  /**
   * Calculate the row metadata for the data set fields needed for the given location.
   *
   * @param location
   * @return The fields metadata for those fields that are mapped against a certain transform (location)
   */
  public IRowMeta getMappedDataSetFieldsRowMeta( PipelineUnitTestSetLocation location ) throws HopPluginException {

    IRowMeta setRowMeta = getSetRowMeta();
    IRowMeta rowMeta = new RowMeta();
    for ( PipelineUnitTestFieldMapping fieldMapping : location.getFieldMappings() ) {
      IValueMeta valueMeta = setRowMeta.searchValueMeta( fieldMapping.getDataSetFieldName() );
      rowMeta.addValueMeta( valueMeta );
    }
    return rowMeta;
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
   * @param name The name to set
   */
  public void setName( String name ) {
    this.name = name;
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
   * @param description The description to set
   */
  public void setDescription( String description ) {
    this.description = description;
  }

  /**
   * Gets folderName
   *
   * @return value of folderName
   */
  public String getFolderName() {
    return folderName;
  }

  /**
   * @param folderName The folderName to set
   */
  public void setFolderName( String folderName ) {
    this.folderName = folderName;
  }

  /**
   * Gets the base filenname
   *
   * @return value of base filename
   */
  public String getBaseFilename() {
    return baseFilename;
  }

  /**
   * @param baseFilename The base filename to set
   */
  public void setBaseFilename( String baseFilename ) {
    this.baseFilename = baseFilename;
  }

  /**
   * Gets fields
   *
   * @return value of fields
   */
  public List<DataSetField> getFields() {
    return fields;
  }

  /**
   * @param fields The fields to set
   */
  public void setFields( List<DataSetField> fields ) {
    this.fields = fields;
  }

  public String getActualDataSetFolder() {
    String folder = Const.NVL(folderName, "");
    if ( StringUtils.isEmpty( folder ) ) {
      folder = getVariable( VARIABLE_HOP_DATASETS_FOLDER );
    }
    if ( StringUtils.isEmpty( folder ) ) {
      // Local folder
      folder = ".";
    } else {
      folder = environmentSubstitute( folder );
    }

    if ( !folder.endsWith( File.separator ) ) {
      folder += File.separator;
    }

    return folder;
  }

  public String getActualDataSetFilename() {
    String filename = getActualDataSetFolder();
    filename += baseFilename;
    return filename;
  }
}
