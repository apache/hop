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

package org.apache.hop.pipeline.transforms.edi2xml;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.iVariables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.w3c.dom.Node;

import java.util.List;

public class Edi2XmlMeta extends BaseTransformMeta implements ITransform {

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
  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {

    try {
      setInputField( XMLHandler.getNodeValue( XMLHandler.getSubNode( transformNode, "inputfield" ) ) );
      setOutputField( XMLHandler.getNodeValue( XMLHandler.getSubNode( transformNode, "outputfield" ) ) );
    } catch ( Exception e ) {
      throw new HopXMLException( "Template Plugin Unable to read transform info from XML node", e );
    }

  }

  @Override
  public void getFields( IRowMeta r, String origin, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) {

    IValueMeta extra = null;

    if ( !Utils.isEmpty( getOutputField() ) ) {
      extra = new ValueMetaString( variables.environmentSubstitute( getOutputField() ) );
      extra.setOrigin( origin );
      r.addValueMeta( extra );
    } else {
      if ( !Utils.isEmpty( getInputField() ) ) {
        extra = r.searchValueMeta( variables.environmentSubstitute( getInputField() ) );
      }
    }

    if ( extra != null ) {
      extra.setStorageType( IValueMeta.STORAGE_TYPE_NORMAL );
    }

  }

  @Override
  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    CheckResult cr;

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, "Transform is receiving input from other transforms.", transformMeta );
      remarks.add( cr );
    } else {
      cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, "No input received from other transforms!", transformMeta );
      remarks.add( cr );
    }

    // is the input field there?
    String realInputField = pipelineMeta.environmentSubstitute( getInputField() );
    if ( prev.searchValueMeta( realInputField ) != null ) {
      cr = new CheckResult( CheckResult.TYPE_RESULT_OK, "Transform is seeing input field: " + realInputField, transformMeta );
      remarks.add( cr );

      if ( prev.searchValueMeta( realInputField ).isString() ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, "Field " + realInputField + " is a string type", transformMeta );
        remarks.add( cr );
      } else {
        cr =
          new CheckResult(
            CheckResult.TYPE_RESULT_OK, "Field " + realInputField + " is not a string type!", transformMeta );
        remarks.add( cr );
      }

    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, "Transform is not seeing input field: "
          + realInputField + "!", transformMeta );
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
  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr,
                                PipelineMeta pipelineMeta, Pipeline disp ) {
    return new Edi2Xml( transformMeta, this, data, cnr, pipelineMeta, disp );
  }

  @Override
  public ITransformData getTransformData() {
    return new Edi2XmlData();
  }

  @Override
  public boolean supportsErrorHandling() {
    return true;
  }

}
