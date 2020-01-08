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

package org.apache.hop.trans.steps.closure;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.annotations.Step;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 19-Sep-2007
 *
 */

@Step( id = "ClosureGenerator", i18nPackageName = "org.apache.hop.trans.steps.closure",
  name = "BaseStep.TypeLongDesc.ClosureGenerator", description = "BaseStep.TypeTooltipDesc.ClosureGenerator",
  categoryDescription = "i18n:org.apache.hop.trans.step:BaseStep.Category.Transform" )
public class ClosureGeneratorMeta extends BaseStepMeta implements StepMetaInterface {
  private boolean rootIdZero;

  private String parentIdFieldName;
  private String childIdFieldName;
  private String distanceFieldName;

  public ClosureGeneratorMeta() {
    super();
  }

  @Override
  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode, metaStore );
  }

  @Override
  public Object clone() {
    ClosureGeneratorMeta retval = (ClosureGeneratorMeta) super.clone();
    return retval;
  }

  private void readData( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    try {
      parentIdFieldName = XMLHandler.getTagValue( stepnode, "parent_id_field" );
      childIdFieldName = XMLHandler.getTagValue( stepnode, "child_id_field" );
      distanceFieldName = XMLHandler.getTagValue( stepnode, "distance_field" );
      rootIdZero = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "is_root_zero" ) );
    } catch ( Exception e ) {
      throw new HopXMLException( "Unable to load step info from XML", e );
    }
  }

  @Override
  public void setDefault() {
  }

  @Override
  public void getFields( RowMetaInterface row, String origin, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, IMetaStore metaStore ) throws HopStepException {
    // The output for the closure table is:
    //
    // - parentId
    // - childId
    // - distance
    //
    // Nothing else.
    //
    RowMetaInterface result = new RowMeta();
    ValueMetaInterface parentValueMeta = row.searchValueMeta( parentIdFieldName );
    if ( parentValueMeta != null ) {
      result.addValueMeta( parentValueMeta );
    }

    ValueMetaInterface childValueMeta = row.searchValueMeta( childIdFieldName );
    if ( childValueMeta != null ) {
      result.addValueMeta( childValueMeta );
    }

    ValueMetaInterface distanceValueMeta = new ValueMetaInteger( distanceFieldName );
    distanceValueMeta.setLength( ValueMetaInterface.DEFAULT_INTEGER_LENGTH );
    result.addValueMeta( distanceValueMeta );

    row.clear();
    row.addRowMeta( result );
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append( "    " ).append( XMLHandler.addTagValue( "parent_id_field", parentIdFieldName ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "child_id_field", childIdFieldName ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "distance_field", distanceFieldName ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "is_root_zero", rootIdZero ) );

    return retval.toString();
  }

  @Override
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    CheckResult cr;

    ValueMetaInterface parentValueMeta = prev.searchValueMeta( parentIdFieldName );
    if ( parentValueMeta != null ) {
      cr =
        new CheckResult(
          CheckResultInterface.TYPE_RESULT_ERROR, "The fieldname of the parent id could not be found.",
          stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult(
          CheckResultInterface.TYPE_RESULT_OK, "The fieldname of the parent id could be found", stepMeta );
      remarks.add( cr );
    }

    ValueMetaInterface childValueMeta = prev.searchValueMeta( childIdFieldName );
    if ( childValueMeta != null ) {
      cr =
        new CheckResult(
          CheckResultInterface.TYPE_RESULT_ERROR, "The fieldname of the child id could not be found.",
          stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult(
          CheckResultInterface.TYPE_RESULT_OK, "The fieldname of the child id could be found", stepMeta );
      remarks.add( cr );
    }
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
                                TransMeta transMeta, Trans trans ) {
    return new ClosureGenerator( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

  @Override
  public StepDataInterface getStepData() {
    return new ClosureGeneratorData();
  }

  /**
   * @return the rootIdZero
   */
  public boolean isRootIdZero() {
    return rootIdZero;
  }

  /**
   * @param rootIdZero the rootIdZero to set
   */
  public void setRootIdZero( boolean rootIdZero ) {
    this.rootIdZero = rootIdZero;
  }

  /**
   * @return the parentIdFieldName
   */
  public String getParentIdFieldName() {
    return parentIdFieldName;
  }

  /**
   * @param parentIdFieldName the parentIdFieldName to set
   */
  public void setParentIdFieldName( String parentIdFieldName ) {
    this.parentIdFieldName = parentIdFieldName;
  }

  /**
   * @return the childIdFieldName
   */
  public String getChildIdFieldName() {
    return childIdFieldName;
  }

  /**
   * @param childIdFieldName the childIdFieldName to set
   */
  public void setChildIdFieldName( String childIdFieldName ) {
    this.childIdFieldName = childIdFieldName;
  }

  /**
   * @return the distanceFieldName
   */
  public String getDistanceFieldName() {
    return distanceFieldName;
  }

  /**
   * @param distanceFieldName the distanceFieldName to set
   */
  public void setDistanceFieldName( String distanceFieldName ) {
    this.distanceFieldName = distanceFieldName;
  }
}
