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

package org.apache.hop.pipeline.transforms.sasinput;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.iVariables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * @author matt
 * @since 06-OCT-2011
 */

public class SasInputMeta extends BaseTransformMeta implements ITransform {
  private static Class<?> PKG = SasInputMeta.class; // for i18n purposes,

  public static final String XML_TAG_FIELD = "field";

  /**
   * The field in which the filename is placed
   */
  private String acceptingField;

  private List<SasInputField> outputFields;

  public SasInputMeta() {
    super(); // allocate BaseTransformMeta
  }

  @Override
  public void setDefault() {
    outputFields = new ArrayList<SasInputField>();
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    try {
      acceptingField = XMLHandler.getTagValue( transformNode, "accept_field" );
      int nrFields = XMLHandler.countNodes( transformNode, XML_TAG_FIELD );
      outputFields = new ArrayList<SasInputField>();
      for ( int i = 0; i < nrFields; i++ ) {
        Node fieldNode = XMLHandler.getSubNodeByNr( transformNode, XML_TAG_FIELD, i );
        outputFields.add( new SasInputField( fieldNode ) );
      }
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "SASInputMeta.Exception.UnableToReadTransformMetaFromXML" ), e );
    }
  }

  public Object clone() {
    SasInputMeta retval = (SasInputMeta) super.clone();
    retval.setOutputFields( new ArrayList<SasInputField>() );
    for ( SasInputField field : outputFields ) {
      retval.getOutputFields().add( field.clone() );
    }
    return retval;
  }

  @Override
  public void getFields( IRowMeta row, String name, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {

    for ( SasInputField field : outputFields ) {
      try {
        IValueMeta valueMeta = ValueMetaFactory.createValueMeta( field.getRename(), field.getType() );
        valueMeta.setLength( field.getLength(), field.getPrecision() );
        valueMeta.setDecimalSymbol( field.getDecimalSymbol() );
        valueMeta.setGroupingSymbol( field.getGroupingSymbol() );
        valueMeta.setConversionMask( field.getConversionMask() );
        valueMeta.setTrimType( field.getTrimType() );
        valueMeta.setOrigin( name );

        row.addValueMeta( valueMeta );
      } catch ( Exception e ) {
        throw new HopTransformException( e );
      }
    }
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "    " + XMLHandler.addTagValue( "accept_field", acceptingField ) );
    for ( SasInputField field : outputFields ) {
      retval.append( XMLHandler.openTag( XML_TAG_FIELD ) );
      retval.append( field.getXML() );
      retval.append( XMLHandler.closeTag( XML_TAG_FIELD ) );
    }

    return retval.toString();
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {

    CheckResult cr;

    if ( Utils.isEmpty( getAcceptingField() ) ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "SASInput.Log.Error.InvalidAcceptingFieldName" ), transformMeta );
      remarks.add( cr );
    }
  }

  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new SasInput( transformMeta, this, data, cnr, tr, pipeline );
  }

  public ITransformData getTransformData() {
    return new SasInputData();
  }

  /**
   * @return Returns the acceptingField.
   */
  public String getAcceptingField() {
    return acceptingField;
  }

  /**
   * @param acceptingField The acceptingField to set.
   */
  public void setAcceptingField( String acceptingField ) {
    this.acceptingField = acceptingField;
  }

  /**
   * @return the outputFields
   */
  public List<SasInputField> getOutputFields() {
    return outputFields;
  }

  /**
   * @param outputFields the outputFields to set
   */
  public void setOutputFields( List<SasInputField> outputFields ) {
    this.outputFields = outputFields;
  }

}
