/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.steps.setvaluefield;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.step.BaseStepMeta;
import org.apache.hop.pipeline.step.StepDataInterface;
import org.apache.hop.pipeline.step.StepInterface;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.step.StepMetaInterface;
import org.w3c.dom.Node;

import java.util.List;

@InjectionSupported( localizationPrefix = "SetValueField.Injection.", groups = { "FIELDS" } )
public class SetValueFieldMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = SetValueFieldMeta.class; // for i18n purposes, needed by Translator!!

  @Injection( name = "FIELD_NAME", group = "FIELDS" )
  private String[] fieldName;

  @Injection( name = "REPLACE_BY_FIELD_VALUE", group = "FIELDS" )
  private String[] replaceByFieldValue;

  public SetValueFieldMeta() {
    super(); // allocate BaseStepMeta
  }

  /**
   * @return Returns the fieldName.
   */
  public String[] getFieldName() {
    return fieldName;
  }

  /**
   * @param fieldName The fieldName to set.
   */
  public void setFieldName( String[] fieldName ) {
    this.fieldName = fieldName;
  }

  /**
   * @return Returns the replaceByFieldValue.
   */
  public String[] getReplaceByFieldValue() {
    return replaceByFieldValue;
  }

  /**
   * @param replaceByFieldValue The replaceByFieldValue to set.
   */
  public void setReplaceByFieldValue( String[] replaceByFieldValue ) {
    this.replaceByFieldValue = replaceByFieldValue;
  }

  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode, metaStore );
  }

  public void allocate( int count ) {
    fieldName = new String[ count ];
    replaceByFieldValue = new String[ count ];
  }

  public Object clone() {
    SetValueFieldMeta retval = (SetValueFieldMeta) super.clone();

    int count = fieldName.length;

    retval.allocate( count );
    System.arraycopy( fieldName, 0, retval.fieldName, 0, count );
    System.arraycopy( replaceByFieldValue, 0, retval.replaceByFieldValue, 0, count );

    return retval;
  }

  private void readData( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    try {
      Node fields = XMLHandler.getSubNode( stepnode, "fields" );
      int count = XMLHandler.countNodes( fields, "field" );

      allocate( count );

      for ( int i = 0; i < count; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );

        fieldName[ i ] = XMLHandler.getTagValue( fnode, "name" );
        replaceByFieldValue[ i ] = XMLHandler.getTagValue( fnode, "replaceby" );
      }
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "SetValueFieldMeta.Exception.UnableToReadStepInfoFromXML" ), e );
    }
  }

  public void setDefault() {
    int count = 0;

    allocate( count );

    for ( int i = 0; i < count; i++ ) {
      fieldName[ i ] = "field" + i;
      replaceByFieldValue[ i ] = "";
    }
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "    <fields>" + Const.CR );

    for ( int i = 0; i < fieldName.length; i++ ) {
      retval.append( "      <field>" + Const.CR );
      retval.append( "        " + XMLHandler.addTagValue( "name", fieldName[ i ] ) );
      retval.append( "        " + XMLHandler.addTagValue( "replaceby", replaceByFieldValue[ i ] ) );
      retval.append( "        </field>" + Const.CR );
    }
    retval.append( "      </fields>" + Const.CR );

    return retval.toString();
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, StepMeta stepMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "SetValueFieldMeta.CheckResult.NoReceivingFieldsError" ), stepMeta );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SetValueFieldMeta.CheckResult.StepReceivingFieldsOK", prev.size() + "" ), stepMeta );
    }
    remarks.add( cr );

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "SetValueFieldMeta.CheckResult.StepRecevingInfoFromOtherSteps" ), stepMeta );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "SetValueFieldMeta.CheckResult.NoInputReceivedError" ), stepMeta );
    }
    remarks.add( cr );

    if ( fieldName == null && fieldName.length == 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "SetValueFieldMeta.CheckResult.FieldsSelectionEmpty" ), stepMeta );
      remarks.add( cr );
    } else {
      for ( int i = 0; i < fieldName.length; i++ ) {
        if ( Utils.isEmpty( replaceByFieldValue[ i ] ) ) {
          cr =
            new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
              PKG, "SetValueFieldMeta.CheckResult.ReplaceByValueMissing", fieldName[ i ], "" + i ), stepMeta );
          remarks.add( cr );
        }
      }
    }
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new SetValueField( stepMeta, stepDataInterface, cnr, pipelineMeta, pipeline );
  }

  public StepDataInterface getStepData() {
    return new SetValueFieldData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }
}
