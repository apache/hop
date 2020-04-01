/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.steps.analyticquery;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Step;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.injection.AfterInjection;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.TransformationType;
import org.apache.hop.pipeline.step.BaseStepMeta;
import org.apache.hop.pipeline.step.StepDataInterface;
import org.apache.hop.pipeline.step.StepInterface;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.step.StepMetaInterface;
import org.w3c.dom.Node;

import java.util.List;

/**
 * @author ngoodman
 * @since 27-jan-2009
 */
@Step( id = "AnalyticQuery", i18nPackageName = "org.apache.hop.pipeline.steps.analyticquery",
  name = "AnalyticQuery.Name", description = "AnalyticQuery.Description",
  categoryDescription = "i18n:org.apache.hop.pipeline.step:BaseStep.Category.Statistics" )
@InjectionSupported( localizationPrefix = "AnalyticQuery.Injection." )
public class AnalyticQueryMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = AnalyticQuery.class; // for i18n purposes, needed by Translator!!

  public static final int TYPE_FUNCT_LEAD = 0;
  public static final int TYPE_FUNCT_LAG = 1;

  public static final String[] typeGroupCode = /* WARNING: DO NOT TRANSLATE THIS. WE ARE SERIOUS, DON'T TRANSLATE! */
    { "LEAD", "LAG", };

  public static final String[] typeGroupLongDesc = {
    BaseMessages.getString( PKG, "AnalyticQueryMeta.TypeGroupLongDesc.LEAD" ),
    BaseMessages.getString( PKG, "AnalyticQueryMeta.TypeGroupLongDesc.LAG" ) };

  /**
   * Fields to partition by ie, CUSTOMER, PRODUCT
   */
  @Injection( name = "GROUP_FIELDS" )
  private String[] groupField;

  private int number_of_fields;
  /** BEGIN arrays (each of size number_of_fields) */

  /**
   * Name of OUTPUT fieldname "MYNEWLEADFUNCTION"
   */
  @Injection( name = "OUTPUT.AGGREGATE_FIELD" )
  private String[] aggregateField;
  /**
   * Name of the input fieldname it operates on "ORDERTOTAL"
   */
  @Injection( name = "OUTPUT.SUBJECT_FIELD" )
  private String[] subjectField;
  /**
   * Aggregate type (LEAD/LAG, etc)
   */
  @Injection( name = "OUTPUT.AGGREGATE_TYPE" )
  private int[] aggregateType;
  /**
   * Offset "N" of how many rows to go forward/back
   */
  @Injection( name = "OUTPUT.VALUE_FIELD" )
  private int[] valueField;

  /**
   * END arrays are one for each configured analytic function
   */

  public AnalyticQueryMeta() {
    super(); // allocate BaseStepMeta
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
   * @return Returns the aggregateTypes.
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
  public int[] getValueField() {
    return valueField;
  }

  /**
   * @param valueField The valueField to set.
   */
  public void setValueField( int[] valueField ) {
    this.valueField = valueField;
  }

  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode );
  }

  public void allocate( int sizegroup, int nrfields ) {
    groupField = new String[ sizegroup ];
    aggregateField = new String[ nrfields ];
    subjectField = new String[ nrfields ];
    aggregateType = new int[ nrfields ];
    valueField = new int[ nrfields ];

    number_of_fields = nrfields;
  }

  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  private void readData( Node stepnode ) throws HopXMLException {
    try {

      Node groupn = XMLHandler.getSubNode( stepnode, "group" );
      Node fields = XMLHandler.getSubNode( stepnode, "fields" );

      int sizegroup = XMLHandler.countNodes( groupn, "field" );
      int nrfields = XMLHandler.countNodes( fields, "field" );

      allocate( sizegroup, nrfields );

      for ( int i = 0; i < sizegroup; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( groupn, "field", i );
        groupField[ i ] = XMLHandler.getTagValue( fnode, "name" );
      }
      for ( int i = 0; i < nrfields; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );
        aggregateField[ i ] = XMLHandler.getTagValue( fnode, "aggregate" );
        subjectField[ i ] = XMLHandler.getTagValue( fnode, "subject" );
        aggregateType[ i ] = getType( XMLHandler.getTagValue( fnode, "type" ) );

        valueField[ i ] = Integer.parseInt( XMLHandler.getTagValue( fnode, "valuefield" ) );
      }

    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "AnalyticQueryMeta.Exception.UnableToLoadStepInfoFromXML" ), e );
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

  public void setDefault() {

    int sizegroup = 0;
    int nrfields = 0;

    allocate( sizegroup, nrfields );
  }

  public void getFields( RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, IMetaStore metaStore ) throws HopStepException {
    // re-assemble a new row of metadata
    //
    RowMetaInterface fields = new RowMeta();

    // Add existing values
    fields.addRowMeta( r );

    // add analytic values
    for ( int i = 0; i < number_of_fields; i++ ) {

      int index_of_subject = -1;
      index_of_subject = r.indexOfValue( subjectField[ i ] );

      // if we found the subjectField in the RowMetaInterface, and we should....
      if ( index_of_subject > -1 ) {
        ValueMetaInterface vmi = r.getValueMeta( index_of_subject ).clone();
        vmi.setOrigin( origin );
        vmi.setName( aggregateField[ i ] );
        fields.addValueMeta( r.size() + i, vmi );
      } else {
        // we have a condition where the subjectField can't be found from the rowMetaInterface
        StringBuilder sbfieldNames = new StringBuilder();
        String[] fieldNames = r.getFieldNames();
        for ( int j = 0; j < fieldNames.length; j++ ) {
          sbfieldNames.append( "[" + fieldNames[ j ] + "]" + ( j < fieldNames.length - 1 ? ", " : "" ) );
        }
        throw new HopStepException( BaseMessages.getString(
          PKG, "AnalyticQueryMeta.Exception.SubjectFieldNotFound", getParentStepMeta().getName(),
          subjectField[ i ], sbfieldNames.toString() ) );
      }
    }

    r.clear();
    // Add back to Row Meta
    r.addRowMeta( fields );
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 500 );

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

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, StepMeta stepMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;

    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "AnalyticQueryMeta.CheckResult.ReceivingInfoOK" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "AnalyticQueryMeta.CheckResult.NoInputError" ), stepMeta );
      remarks.add( cr );
    }
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new AnalyticQuery( stepMeta, stepDataInterface, cnr, pipelineMeta, pipeline );
  }

  public StepDataInterface getStepData() {
    return new AnalyticQueryData();
  }

  public int getNumberOfFields() {
    return number_of_fields;
  }

  public void setNumberOfFields( int number_of_fields ) {
    this.number_of_fields = number_of_fields;
  }

  public TransformationType[] getSupportedTransformationTypes() {
    return new TransformationType[] { TransformationType.Normal, };
  }

  /**
   * If we use injection we can have different arrays lengths.
   * We need synchronize them for consistency behavior with UI
   */
  @AfterInjection
  public void afterInjectionSynchronization() {
    int nrFields = ( subjectField == null ) ? -1 : subjectField.length;
    if ( nrFields <= 0 ) {
      return;
    }
    String[][] rtn = Utils.normalizeArrays( nrFields, aggregateField );
    aggregateField = rtn[ 0 ];

    int[][] rtnInt = Utils.normalizeArrays( nrFields, aggregateType, valueField );
    aggregateType = rtnInt[ 0 ];
    valueField = rtnInt[ 1 ];
  }

}
