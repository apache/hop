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

package org.apache.hop.pipeline.transforms.groupby;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNone;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.w3c.dom.Node;

import java.util.List;

/**
 * Created on 02-jun-2003
 */

public class GroupByMeta extends BaseTransformMeta implements ITransformMeta<GroupBy, GroupByData> {

  private static Class<?> PKG = GroupByMeta.class; // for i18n purposes, needed by Translator!!

  public static final int TYPE_GROUP_NONE = 0;

  public static final int TYPE_GROUP_SUM = 1;

  public static final int TYPE_GROUP_AVERAGE = 2;

  public static final int TYPE_GROUP_MEDIAN = 3;

  public static final int TYPE_GROUP_PERCENTILE = 4;

  public static final int TYPE_GROUP_MIN = 5;

  public static final int TYPE_GROUP_MAX = 6;

  public static final int TYPE_GROUP_COUNT_ALL = 7;

  public static final int TYPE_GROUP_CONCAT_COMMA = 8;

  public static final int TYPE_GROUP_FIRST = 9;

  public static final int TYPE_GROUP_LAST = 10;

  public static final int TYPE_GROUP_FIRST_INCL_NULL = 11;

  public static final int TYPE_GROUP_LAST_INCL_NULL = 12;

  public static final int TYPE_GROUP_CUMULATIVE_SUM = 13;

  public static final int TYPE_GROUP_CUMULATIVE_AVERAGE = 14;

  public static final int TYPE_GROUP_STANDARD_DEVIATION = 15;

  public static final int TYPE_GROUP_CONCAT_STRING = 16;

  public static final int TYPE_GROUP_COUNT_DISTINCT = 17;

  public static final int TYPE_GROUP_COUNT_ANY = 18;

  public static final int TYPE_GROUP_STANDARD_DEVIATION_SAMPLE = 19;

  public static final int TYPE_GROUP_PERCENTILE_NEAREST_RANK = 20;

  public static final String[] typeGroupCode = /* WARNING: DO NOT TRANSLATE THIS. WE ARE SERIOUS, DON'T TRANSLATE! */
    {
      "-", "SUM", "AVERAGE", "MEDIAN", "PERCENTILE", "MIN", "MAX", "COUNT_ALL", "CONCAT_COMMA", "FIRST", "LAST",
      "FIRST_INCL_NULL", "LAST_INCL_NULL", "CUM_SUM", "CUM_AVG", "STD_DEV", "CONCAT_STRING", "COUNT_DISTINCT",
      "COUNT_ANY", "STD_DEV_SAMPLE", "PERCENTILE_NEAREST_RANK" };

  public static final String[] typeGroupLongDesc = {
    "-", BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.SUM" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.AVERAGE" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.MEDIAN" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.PERCENTILE" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.MIN" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.MAX" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.CONCAT_ALL" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.CONCAT_COMMA" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.FIRST" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.LAST" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.FIRST_INCL_NULL" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.LAST_INCL_NULL" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.CUMUMALTIVE_SUM" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.CUMUMALTIVE_AVERAGE" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.STANDARD_DEVIATION" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.CONCAT_STRING" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.COUNT_DISTINCT" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.COUNT_ANY" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.STANDARD_DEVIATION_SAMPLE" ),
    BaseMessages.getString( PKG, "GroupByMeta.TypeGroupLongDesc.PERCENTILE_NEAREST_RANK" )
  };


  /**
   * All rows need to pass, adding an extra row at the end of each group/block.
   */
  private boolean passAllRows;

  /**
   * Directory to store the temp files
   */
  private String directory;

  /**
   * Temp files prefix...
   */
  private String prefix;

  /**
   * Indicate that some rows don't need to be considered : TODO: make work in GUI & worker
   */
  private boolean aggregateIgnored;

  /**
   * name of the boolean field that indicates we need to ignore the row : TODO: make work in GUI & worker
   */
  private String aggregateIgnoredField;

  /**
   * Fields to group over
   */
  private String[] groupField;

  /**
   * Name of aggregate field
   */
  private String[] aggregateField;

  /**
   * Field name to group over
   */
  private String[] subjectField;

  /**
   * Type of aggregate
   */
  private int[] aggregateType;

  /**
   * Value to use as separator for ex
   */
  private String[] valueField;

  /**
   * Add a linenr in the group, resetting to 0 in a new group.
   */
  private boolean addingLineNrInGroup;

  /**
   * The fieldname that will contain the added integer field
   */
  private String lineNrInGroupField;

  /**
   * Flag to indicate that we always give back one row. Defaults to true for existing pipelines.
   */
  private boolean alwaysGivingBackOneRow;

  public GroupByMeta() {
    super(); // allocate BaseTransformMeta
  }

  /**
   * @return Returns the aggregateField.
   */
  public String[] getAggregateField() {
    return aggregateField;
  }

  /**
   * @param aggregateField The aggregateField to set.
   */
  public void setAggregateField( String[] aggregateField ) {
    this.aggregateField = aggregateField;
  }

  /**
   * @return Returns the aggregateIgnored.
   */
  public boolean isAggregateIgnored() {
    return aggregateIgnored;
  }

  /**
   * @param aggregateIgnored The aggregateIgnored to set.
   */
  public void setAggregateIgnored( boolean aggregateIgnored ) {
    this.aggregateIgnored = aggregateIgnored;
  }

  /**
   * @return Returns the aggregateIgnoredField.
   */
  public String getAggregateIgnoredField() {
    return aggregateIgnoredField;
  }

  /**
   * @param aggregateIgnoredField The aggregateIgnoredField to set.
   */
  public void setAggregateIgnoredField( String aggregateIgnoredField ) {
    this.aggregateIgnoredField = aggregateIgnoredField;
  }

  /**
   * @return Returns the aggregateType.
   */
  public int[] getAggregateType() {
    return aggregateType;
  }

  /**
   * @param aggregateType The aggregateType to set.
   */
  public void setAggregateType( int[] aggregateType ) {
    this.aggregateType = aggregateType;
  }

  /**
   * @return Returns the groupField.
   */
  public String[] getGroupField() {
    return groupField;
  }

  /**
   * @param groupField The groupField to set.
   */
  public void setGroupField( String[] groupField ) {
    this.groupField = groupField;
  }

  /**
   * @return Returns the passAllRows.
   */
  public boolean passAllRows() {
    return passAllRows;
  }

  /**
   * @param passAllRows The passAllRows to set.
   */
  public void setPassAllRows( boolean passAllRows ) {
    this.passAllRows = passAllRows;
  }

  /**
   * @return Returns the subjectField.
   */
  public String[] getSubjectField() {
    return subjectField;
  }

  /**
   * @param subjectField The subjectField to set.
   */
  public void setSubjectField( String[] subjectField ) {
    this.subjectField = subjectField;
  }

  /**
   * @return Returns the valueField.
   */
  public String[] getValueField() {
    return valueField;
  }

  /**
   * @param valueField The valueField to set.
   */
  public void setValueField( String[] valueField ) {
    this.valueField = valueField;
  }

  @Override
  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
  }

  public void allocate( int sizegroup, int nrFields ) {
    groupField = new String[ sizegroup ];
    aggregateField = new String[ nrFields ];
    subjectField = new String[ nrFields ];
    aggregateType = new int[ nrFields ];
    valueField = new String[ nrFields ];
  }

  @Override
  public Object clone() {
    GroupByMeta retval = (GroupByMeta) super.clone();

    int szGroup = 0, szFields = 0;
    if ( groupField != null ) {
      szGroup = groupField.length;
    }
    if ( valueField != null ) {
      szFields = valueField.length;
    }
    retval.allocate( szGroup, szFields );

    System.arraycopy( groupField, 0, retval.groupField, 0, szGroup );
    System.arraycopy( aggregateField, 0, retval.aggregateField, 0, szFields );
    System.arraycopy( subjectField, 0, retval.subjectField, 0, szFields );
    System.arraycopy( aggregateType, 0, retval.aggregateType, 0, szFields );
    System.arraycopy( valueField, 0, retval.valueField, 0, szFields );

    return retval;
  }

  private void readData( Node transformNode ) throws HopXMLException {
    try {
      passAllRows = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "all_rows" ) );
      aggregateIgnored = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "ignore_aggregate" ) );
      aggregateIgnoredField = XMLHandler.getTagValue( transformNode, "field_ignore" );

      directory = XMLHandler.getTagValue( transformNode, "directory" );
      prefix = XMLHandler.getTagValue( transformNode, "prefix" );

      addingLineNrInGroup = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "add_linenr" ) );
      lineNrInGroupField = XMLHandler.getTagValue( transformNode, "linenr_fieldname" );

      Node groupn = XMLHandler.getSubNode( transformNode, "group" );
      Node fields = XMLHandler.getSubNode( transformNode, "fields" );

      int sizegroup = XMLHandler.countNodes( groupn, "field" );
      int nrFields = XMLHandler.countNodes( fields, "field" );

      allocate( sizegroup, nrFields );

      for ( int i = 0; i < sizegroup; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( groupn, "field", i );
        groupField[ i ] = XMLHandler.getTagValue( fnode, "name" );
      }

      boolean hasNumberOfValues = false;
      for ( int i = 0; i < nrFields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );
        aggregateField[ i ] = XMLHandler.getTagValue( fnode, "aggregate" );
        subjectField[ i ] = XMLHandler.getTagValue( fnode, "subject" );
        aggregateType[ i ] = getType( XMLHandler.getTagValue( fnode, "type" ) );

        if ( aggregateType[ i ] == TYPE_GROUP_COUNT_ALL
          || aggregateType[ i ] == TYPE_GROUP_COUNT_DISTINCT || aggregateType[ i ] == TYPE_GROUP_COUNT_ANY ) {
          hasNumberOfValues = true;
        }

        valueField[ i ] = XMLHandler.getTagValue( fnode, "valuefield" );
      }

      String giveBackRow = XMLHandler.getTagValue( transformNode, "give_back_row" );
      if ( Utils.isEmpty( giveBackRow ) ) {
        alwaysGivingBackOneRow = hasNumberOfValues;
      } else {
        alwaysGivingBackOneRow = "Y".equalsIgnoreCase( giveBackRow );
      }
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "GroupByMeta.Exception.UnableToLoadTransformMetaFromXML" ), e );
    }
  }

  public static final int getType( String desc ) {
    for ( int i = 0; i < typeGroupCode.length; i++ ) {
      if ( typeGroupCode[ i ].equalsIgnoreCase( desc ) ) {
        return i;
      }
    }
    for ( int i = 0; i < typeGroupLongDesc.length; i++ ) {
      if ( typeGroupLongDesc[ i ].equalsIgnoreCase( desc ) ) {
        return i;
      }
    }
    return 0;
  }

  public static final String getTypeDesc( int i ) {
    if ( i < 0 || i >= typeGroupCode.length ) {
      return null;
    }
    return typeGroupCode[ i ];
  }

  public static final String getTypeDescLong( int i ) {
    if ( i < 0 || i >= typeGroupLongDesc.length ) {
      return null;
    }
    return typeGroupLongDesc[ i ];
  }

  @Override
  public void setDefault() {
    directory = "%%java.io.tmpdir%%";
    prefix = "grp";

    passAllRows = false;
    aggregateIgnored = false;
    aggregateIgnoredField = null;

    int sizeGroup = 0;
    int numberOfFields = 0;

    allocate( sizeGroup, numberOfFields );
  }

  @Override
  public void getFields( IRowMeta rowMeta, String origin, IRowMeta[] info, TransformMeta nextTransform,
                         IVariables variables, IMetaStore metaStore ) {
    // re-assemble a new row of metadata
    //
    IRowMeta fields = new RowMeta();

    if ( !passAllRows ) {
      // Add the grouping fields in the correct order...
      //
      for ( int i = 0; i < groupField.length; i++ ) {
        IValueMeta valueMeta = rowMeta.searchValueMeta( groupField[ i ] );
        if ( valueMeta != null ) {
          fields.addValueMeta( valueMeta );
        }
      }
    } else {
      // Add all the original fields from the incoming row meta
      //
      fields.addRowMeta( rowMeta );
    }

    // Re-add aggregates
    //
    for ( int i = 0; i < subjectField.length; i++ ) {
      IValueMeta subj = rowMeta.searchValueMeta( subjectField[ i ] );
      if ( subj != null || aggregateType[ i ] == TYPE_GROUP_COUNT_ANY ) {
        String valueName = aggregateField[ i ];
        int valueType = IValueMeta.TYPE_NONE;
        int length = -1;
        int precision = -1;

        switch ( aggregateType[ i ] ) {
          case TYPE_GROUP_SUM:
          case TYPE_GROUP_AVERAGE:
          case TYPE_GROUP_CUMULATIVE_SUM:
          case TYPE_GROUP_CUMULATIVE_AVERAGE:
          case TYPE_GROUP_FIRST:
          case TYPE_GROUP_LAST:
          case TYPE_GROUP_FIRST_INCL_NULL:
          case TYPE_GROUP_LAST_INCL_NULL:
          case TYPE_GROUP_MIN:
          case TYPE_GROUP_MAX:
            valueType = subj.getType();
            break;
          case TYPE_GROUP_COUNT_DISTINCT:
          case TYPE_GROUP_COUNT_ANY:
          case TYPE_GROUP_COUNT_ALL:
            valueType = IValueMeta.TYPE_INTEGER;
            break;
          case TYPE_GROUP_CONCAT_COMMA:
            valueType = IValueMeta.TYPE_STRING;
            break;
          case TYPE_GROUP_STANDARD_DEVIATION:
          case TYPE_GROUP_MEDIAN:
          case TYPE_GROUP_STANDARD_DEVIATION_SAMPLE:
          case TYPE_GROUP_PERCENTILE:
          case TYPE_GROUP_PERCENTILE_NEAREST_RANK:
            valueType = IValueMeta.TYPE_NUMBER;
            break;
          case TYPE_GROUP_CONCAT_STRING:
            valueType = IValueMeta.TYPE_STRING;
            break;
          default:
            break;
        }

        // Change type from integer to number in case off averages for cumulative average
        //
        if ( aggregateType[ i ] == TYPE_GROUP_CUMULATIVE_AVERAGE && valueType == IValueMeta.TYPE_INTEGER ) {
          valueType = IValueMeta.TYPE_NUMBER;
          precision = -1;
          length = -1;
        } else if ( aggregateType[ i ] == TYPE_GROUP_COUNT_ALL
          || aggregateType[ i ] == TYPE_GROUP_COUNT_DISTINCT || aggregateType[ i ] == TYPE_GROUP_COUNT_ANY ) {
          length = IValueMeta.DEFAULT_INTEGER_LENGTH;
          precision = 0;
        } else if ( aggregateType[ i ] == TYPE_GROUP_SUM
          && valueType != IValueMeta.TYPE_INTEGER && valueType != IValueMeta.TYPE_NUMBER
          && valueType != IValueMeta.TYPE_BIGNUMBER ) {
          // If it ain't numeric, we change it to Number
          //
          valueType = IValueMeta.TYPE_NUMBER;
          precision = -1;
          length = -1;
        }

        if ( valueType != IValueMeta.TYPE_NONE ) {
          IValueMeta v;
          try {
            v = ValueMetaFactory.createValueMeta( valueName, valueType );
          } catch ( HopPluginException e ) {
            v = new ValueMetaNone( valueName );
          }
          v.setOrigin( origin );
          v.setLength( length, precision );

          if ( subj != null ) {
            v.setConversionMask( subj.getConversionMask() );
          }

          fields.addValueMeta( v );
        }
      }
    }

    if ( passAllRows ) {
      // If we pass all rows, we can add a line nr in the group...
      if ( addingLineNrInGroup && !Utils.isEmpty( lineNrInGroupField ) ) {
        IValueMeta lineNr = new ValueMetaInteger( lineNrInGroupField );
        lineNr.setLength( IValueMeta.DEFAULT_INTEGER_LENGTH, 0 );
        lineNr.setOrigin( origin );
        fields.addValueMeta( lineNr );
      }

    }

    // Now that we have all the fields we want, we should clear the original row and replace the values...
    //
    rowMeta.clear();
    rowMeta.addRowMeta( fields );
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder( 500 );

    retval.append( "      " ).append( XMLHandler.addTagValue( "all_rows", passAllRows ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "ignore_aggregate", aggregateIgnored ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "field_ignore", aggregateIgnoredField ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "directory", directory ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "prefix", prefix ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "add_linenr", addingLineNrInGroup ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "linenr_fieldname", lineNrInGroupField ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "give_back_row", alwaysGivingBackOneRow ) );

    retval.append( "      <group>" ).append( Const.CR );
    for ( int i = 0; i < groupField.length; i++ ) {
      retval.append( "        <field>" ).append( Const.CR );
      retval.append( "          " ).append( XMLHandler.addTagValue( "name", groupField[ i ] ) );
      retval.append( "        </field>" ).append( Const.CR );
    }
    retval.append( "      </group>" ).append( Const.CR );

    retval.append( "      <fields>" ).append( Const.CR );
    for ( int i = 0; i < subjectField.length; i++ ) {
      retval.append( "        <field>" ).append( Const.CR );
      retval.append( "          " ).append( XMLHandler.addTagValue( "aggregate", aggregateField[ i ] ) );
      retval.append( "          " ).append( XMLHandler.addTagValue( "subject", subjectField[ i ] ) );
      retval.append( "          " ).append( XMLHandler.addTagValue( "type", getTypeDesc( aggregateType[ i ] ) ) );
      retval.append( "          " ).append( XMLHandler.addTagValue( "valuefield", valueField[ i ] ) );
      retval.append( "        </field>" ).append( Const.CR );
    }
    retval.append( "      </fields>" ).append( Const.CR );

    return retval.toString();
  }

  @Override
  public void check( List<ICheckResult> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, IVariables variables,
                     IMetaStore metaStore ) {
    CheckResult cr;

    if ( input.length > 0 ) {
      cr =
        new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "GroupByMeta.CheckResult.ReceivingInfoOK" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( ICheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "GroupByMeta.CheckResult.NoInputError" ), transformMeta );
      remarks.add( cr );
    }
  }

  @Override
  public ITransform createTransform( TransformMeta transformMeta, GroupByData data, int cnr,
                                     PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new GroupBy( transformMeta, this, data, cnr, pipelineMeta, pipeline );
  }


  @Override
  public GroupByData getTransformData() {
    return new GroupByData();
  }

  /**
   * @return Returns the directory.
   */
  public String getDirectory() {
    return directory;
  }

  /**
   * @param directory The directory to set.
   */
  public void setDirectory( String directory ) {
    this.directory = directory;
  }

  /**
   * @return Returns the prefix.
   */
  public String getPrefix() {
    return prefix;
  }

  /**
   * @param prefix The prefix to set.
   */
  public void setPrefix( String prefix ) {
    this.prefix = prefix;
  }

  /**
   * @return the addingLineNrInGroup
   */
  public boolean isAddingLineNrInGroup() {
    return addingLineNrInGroup;
  }

  /**
   * @param addingLineNrInGroup the addingLineNrInGroup to set
   */
  public void setAddingLineNrInGroup( boolean addingLineNrInGroup ) {
    this.addingLineNrInGroup = addingLineNrInGroup;
  }

  /**
   * @return the lineNrInGroupField
   */
  public String getLineNrInGroupField() {
    return lineNrInGroupField;
  }

  /**
   * @param lineNrInGroupField the lineNrInGroupField to set
   */
  public void setLineNrInGroupField( String lineNrInGroupField ) {
    this.lineNrInGroupField = lineNrInGroupField;
  }

  /**
   * @return the alwaysGivingBackOneRow
   */
  public boolean isAlwaysGivingBackOneRow() {
    return alwaysGivingBackOneRow;
  }

  /**
   * @param alwaysGivingBackOneRow the alwaysGivingBackOneRow to set
   */
  public void setAlwaysGivingBackOneRow( boolean alwaysGivingBackOneRow ) {
    this.alwaysGivingBackOneRow = alwaysGivingBackOneRow;
  }

  @Override
  public PipelineMeta.PipelineType[] getSupportedPipelineTypes() {
    return new PipelineMeta.PipelineType[] { PipelineMeta.PipelineType.Normal };
  }
}
