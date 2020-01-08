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

package org.apache.hop.trans.steps.dummy;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Step;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaAndData;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.BaseStepMeta;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepDialogInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.step.StepMetaInterface;
import org.eclipse.swt.widgets.Shell;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 02-jun-2003
 *
 */

@Step( id = "DummyStep",
  image = "deprecated.svg",
  i18nPackageName = "be.ibridge.kettle.dummy",
  name = "DummyPlugin.Step.Name",
  description = "DummyPlugin.Step.Description",
  categoryDescription = "i18n:org.apache.hop.trans.step:BaseStep.Category.Deprecated",
  suggestion = "DummyPlugin.Step.SuggestedStep" )
public class DummyPluginMeta extends BaseStepMeta implements StepMetaInterface {
  private ValueMetaAndData value;

  public DummyPluginMeta() {
    super(); // allocate BaseStepInfo
  }

  /**
   * @return Returns the value.
   */
  public ValueMetaAndData getValue() {
    return value;
  }

  /**
   * @param value The value to set.
   */
  public void setValue( ValueMetaAndData value ) {
    this.value = value;
  }

  @Override
  public String getXML() throws HopException {
    String retval = "";

    retval += "    <values>" + Const.CR;
    if ( value != null ) {
      retval += value.getXML();
    }
    retval += "      </values>" + Const.CR;

    return retval;
  }

  @Override public void getFields( RowMetaInterface inputRowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, IMetaStore metaStore ) throws HopStepException {

    if ( value != null ) {
      ValueMetaInterface v = value.getValueMeta();
      v.setOrigin( origin );

      inputRowMeta.addValueMeta( v );
    }
  }

  @Override
  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  @Override public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    try {
      value = new ValueMetaAndData();

      Node valnode = XMLHandler.getSubNode( stepnode, "values", "value" );
      if ( valnode != null ) {
        System.out.println( "reading value in " + valnode );
        value.loadXML( valnode );
      }
    } catch ( Exception e ) {
      throw new HopXMLException( "Unable to read step info from XML node", e );
    }
  }

  @Override
  public void setDefault() {
    value = new ValueMetaAndData( new ValueMetaNumber( "valuename" ), new Double( 123.456 ) );
    value.getValueMeta().setLength( 12 );
    value.getValueMeta().setPrecision( 4 );
  }

  @Override
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    super.check( remarks, transMeta, stepMeta, prev, input, output, info, space, metaStore );
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr = new CheckResult( CheckResult.TYPE_RESULT_WARNING, "Not receiving any fields from previous steps!", stepMeta );
      remarks.add( cr );
    } else {
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, "Step is connected to previous one, receiving " + prev.size() + " fields", stepMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, "Step is receiving info from other steps.", stepMeta );
      remarks.add( cr );
    } else {
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, "No input received from other steps!", stepMeta );
      remarks.add( cr );
    }
  }

  public StepDialogInterface getDialog( Shell shell, StepMetaInterface meta, TransMeta transMeta, String name ) {
    return new DummyPluginDialog( shell, meta, transMeta, name );
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta, Trans disp ) {
    return new DummyPlugin( stepMeta, stepDataInterface, cnr, transMeta, disp );
  }

  @Override
  public StepDataInterface getStepData() {
    return new DummyPluginData();
  }
}
