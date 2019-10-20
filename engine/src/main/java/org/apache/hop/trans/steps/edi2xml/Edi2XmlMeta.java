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

package org.apache.hop.trans.steps.edi2xml;

import java.util.List;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.repository.ObjectId;
import org.apache.hop.repository.Repository;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

public class Edi2XmlMeta extends BaseStepMeta implements StepMetaInterface {

  private static Class<?> PKG = Edi2XmlMeta.class; // for i18n purposes

  private String outputField;
  private String inputField;

  public Edi2XmlMeta() {
    super();
  }

  public String getInputField() {
    return inputField;
  }

  public void setInputField( String inputField ) {
    this.inputField = inputField;
  }

  public String getOutputField() {
    return outputField;
  }

  public void setOutputField( String outputField ) {
    this.outputField = outputField;
  }

  @Override
  public String getXML() throws HopValueException {
    StringBuilder retval = new StringBuilder();

    retval.append( "   " + XMLHandler.addTagValue( "inputfield", inputField ) );
    retval.append( "   " + XMLHandler.addTagValue( "outputfield", outputField ) );

    return retval.toString();
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws HopXMLException {

    try {
      setInputField( XMLHandler.getNodeValue( XMLHandler.getSubNode( stepnode, "inputfield" ) ) );
      setOutputField( XMLHandler.getNodeValue( XMLHandler.getSubNode( stepnode, "outputfield" ) ) );
    } catch ( Exception e ) {
      throw new HopXMLException( "Template Plugin Unable to read step info from XML node", e );
    }

  }

  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases ) throws HopException {
    try {
      inputField = rep.getStepAttributeString( id_step, "inputfield" );
      outputField = rep.getStepAttributeString( id_step, "outputfield" );
    } catch ( Exception e ) {
      throw new HopException( BaseMessages
        .getString( PKG, "Edi2Xml.Exception.UnexpectedErrorInReadingStepInfo" ), e );
    }
  }

  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step ) throws HopException {
    try {
      rep.saveStepAttribute( id_transformation, id_step, "inputfield", inputField );
      rep.saveStepAttribute( id_transformation, id_step, "outputfield", outputField );
    } catch ( Exception e ) {
      throw new HopException( BaseMessages
        .getString( PKG, "Edi2Xml.Exception.UnableToSaveStepInfoToRepository" )
        + id_step, e );
    }
  }

  @Override
  public void getFields( RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep,
    VariableSpace space, Repository repository, IMetaStore metaStore ) {

    ValueMetaInterface extra = null;

    if ( !Utils.isEmpty( getOutputField() ) ) {
      extra = new ValueMetaString( space.environmentSubstitute( getOutputField() ) );
      extra.setOrigin( origin );
      r.addValueMeta( extra );
    } else {
      if ( !Utils.isEmpty( getInputField() ) ) {
        extra = r.searchValueMeta( space.environmentSubstitute( getInputField() ) );
      }
    }

    if ( extra != null ) {
      extra.setStorageType( ValueMetaInterface.STORAGE_TYPE_NORMAL );
    }

  }

  @Override
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta,
    RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
    Repository repository, IMetaStore metaStore ) {
    CheckResult cr;

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, "Step is receiving input from other steps.", stepMeta );
      remarks.add( cr );
    } else {
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, "No input received from other steps!", stepMeta );
      remarks.add( cr );
    }

    // is the input field there?
    String realInputField = transMeta.environmentSubstitute( getInputField() );
    if ( prev.searchValueMeta( realInputField ) != null ) {
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, "Step is seeing input field: " + realInputField, stepMeta );
      remarks.add( cr );

      if ( prev.searchValueMeta( realInputField ).isString() ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, "Field " + realInputField + " is a string type", stepMeta );
        remarks.add( cr );
      } else {
        cr =
          new CheckResult(
            CheckResult.TYPE_RESULT_OK, "Field " + realInputField + " is not a string type!", stepMeta );
        remarks.add( cr );
      }

    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, "Step is not seeing input field: "
          + realInputField + "!", stepMeta );
      remarks.add( cr );
    }

  }

  @Override
  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  @Override
  public void setDefault() {
    outputField = "edi_xml";
    inputField = "";
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
    TransMeta transMeta, Trans disp ) {
    return new Edi2Xml( stepMeta, stepDataInterface, cnr, transMeta, disp );
  }

  @Override
  public StepDataInterface getStepData() {
    return new Edi2XmlData();
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

}
