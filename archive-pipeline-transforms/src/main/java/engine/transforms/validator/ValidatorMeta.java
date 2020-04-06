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

package org.apache.hop.pipeline.transforms.validator;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.iVariables;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.TransformIOMetaInterface;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.apache.hop.pipeline.transform.errorhandling.StreamIcon;
import org.apache.hop.pipeline.transform.errorhandling.StreamInterface;
import org.apache.hop.pipeline.transform.errorhandling.StreamInterface.StreamType;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Contains the meta-data for the Validator transform: calculates predefined formula's
 * <p>
 * Created on 08-sep-2005
 */
@InjectionSupported( localizationPrefix = "Validator.Injection.", groups = { "VALIDATIONS" } )
public class ValidatorMeta extends BaseTransformMeta implements ITransform {
  private static Class<?> PKG = ValidatorMeta.class; // for i18n purposes, needed by Translator!!

  /**
   * The calculations to be performed
   */
  @InjectionDeep
  private List<Validation> validations;

  /**
   * Checkbox to have all rules validated, with all the errors in the output
   */
  @Injection( name = "VALIDATE_ALL" )
  private boolean validatingAll;

  /**
   * If enabled, it concatenates all encountered errors with the selected separator
   */
  @Injection( name = "CONCATENATE_ERRORS" )
  private boolean concatenatingErrors;

  /**
   * The concatenation separator
   */
  @Injection( name = "CONCATENATION_SEPARATOR" )
  private String concatenationSeparator;

  public void allocate( int nrValidations ) {
    validations = new ArrayList<Validation>( nrValidations );
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    int nrCalcs = XMLHandler.countNodes( transformNode, Validation.XML_TAG );
    allocate( nrCalcs );
    validatingAll = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "validate_all" ) );
    concatenatingErrors = "Y".equalsIgnoreCase( XMLHandler.getTagValue( transformNode, "concat_errors" ) );
    concatenationSeparator = XMLHandler.getTagValue( transformNode, "concat_separator" );

    for ( int i = 0; i < nrCalcs; i++ ) {
      Node calcnode = XMLHandler.getSubNodeByNr( transformNode, Validation.XML_TAG, i );
      validations.add( new Validation( calcnode ) );
    }
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append( XMLHandler.addTagValue( "validate_all", validatingAll ) );
    retval.append( XMLHandler.addTagValue( "concat_errors", concatenatingErrors ) );
    retval.append( XMLHandler.addTagValue( "concat_separator", concatenationSeparator ) );

    for ( int i = 0; i < validations.size(); i++ ) {
      retval.append( "       " ).append( validations.get( i ).getXML() ).append( Const.CR );
    }

    return retval.toString();
  }

  public boolean equals( Object obj ) {
    if ( obj != null && ( obj.getClass().equals( this.getClass() ) ) ) {
      ValidatorMeta m = (ValidatorMeta) obj;
      return ( getXML() == m.getXML() );
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash( validatingAll, concatenatingErrors, concatenationSeparator, validations );
  }

  public Object clone() {
    ValidatorMeta retval = (ValidatorMeta) super.clone();
    if ( validations != null ) {
      int valSize = validations.size();
      retval.allocate( valSize );
      for ( int i = 0; i < valSize; i++ ) {
        retval.validations.add( validations.get( i ).clone() );
      }
    } else {
      retval.allocate( 0 );
    }
    return retval;
  }

  public void setDefault() {
    validations = new ArrayList<Validation>();
    concatenationSeparator = "|";
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "ValidatorMeta.CheckResult.ExpectedInputError" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "ValidatorMeta.CheckResult.FieldsReceived", "" + prev.size() ), transformMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "ValidatorMeta.CheckResult.ExpectedInputOk" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "ValidatorMeta.CheckResult.ExpectedInputError" ), transformMeta );
      remarks.add( cr );
    }
  }

  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new Validator( transformMeta, this, data, cnr, tr, pipeline );
  }

  public ITransformData getTransformData() {
    return new ValidatorData();
  }

  public boolean supportsErrorHandling() {
    return true;
  }

  /**
   * @return the validations
   */
  public List<Validation> getValidations() {
    return validations;
  }

  /**
   * @param validations the validations to set
   */
  public void setValidations( List<Validation> validations ) {
    this.validations = validations;
  }

  public boolean excludeFromRowLayoutVerification() {
    return true;
  }

  /**
   * @return the validatingAll
   */
  public boolean isValidatingAll() {
    return validatingAll;
  }

  /**
   * @param validatingAll the validatingAll to set
   */
  public void setValidatingAll( boolean validatingAll ) {
    this.validatingAll = validatingAll;
  }

  /**
   * @return the concatenatingErrors
   */
  public boolean isConcatenatingErrors() {
    return concatenatingErrors;
  }

  /**
   * @param concatenatingErrors the concatenatingErrors to set
   */
  public void setConcatenatingErrors( boolean concatenatingErrors ) {
    this.concatenatingErrors = concatenatingErrors;
  }

  /**
   * @return the concatenationSeparator
   */
  public String getConcatenationSeparator() {
    return concatenationSeparator;
  }

  /**
   * @param concatenationSeparator the concatenationSeparator to set
   */
  public void setConcatenationSeparator( String concatenationSeparator ) {
    this.concatenationSeparator = concatenationSeparator;
  }

  /**
   * Returns the Input/Output metadata for this transform.
   */
  public TransformIOMetaInterface getTransformIOMeta() {
    TransformIOMetaInterface ioMeta = super.getTransformIOMeta( false );
    if ( ioMeta == null ) {

      ioMeta = new TransformIOMeta( true, true, false, false, true, false );

      // Add the info sources...
      //
      for ( Validation validation : validations ) {
        StreamInterface stream =
          new Stream( StreamType.INFO, validation.getSourcingTransform(), BaseMessages
            .getString( PKG, "ValidatorMeta.InfoStream.ValidationInput.Description", Const.NVL( validation
              .getName(), "" ) ), StreamIcon.INFO, validation );
        ioMeta.addStream( stream );
      }
      setTransformIOMeta( ioMeta );
    }

    return ioMeta;
  }

  @Override
  public void searchInfoAndTargetTransforms( List<TransformMeta> transforms ) {
    List<StreamInterface> infoStreams = getTransformIOMeta().getInfoStreams();
    for ( StreamInterface stream : infoStreams ) {
      Validation validation = (Validation) stream.getSubject();
      TransformMeta transformMeta = TransformMeta.findTransform( transforms, validation.getSourcingTransformName() );
      validation.setSourcingTransform( transformMeta );
    }
    resetTransformIoMeta();
  }

  private static StreamInterface newValidation = new Stream( StreamType.INFO, null, BaseMessages.getString(
    PKG, "ValidatorMeta.NewValidation.Description" ), StreamIcon.INFO, null );

  public List<StreamInterface> getOptionalStreams() {
    List<StreamInterface> list = new ArrayList<StreamInterface>();

    list.add( newValidation );

    return list;
  }

  public void handleStreamSelection( StreamInterface stream ) {
    // A hack to prevent us from losing information in the Pipeline UI because
    // of the resetTransformIoMeta() call at the end of this method.
    //
    List<StreamInterface> streams = getTransformIOMeta().getInfoStreams();
    for ( int i = 0; i < validations.size(); i++ ) {
      validations.get( i ).setSourcingTransform( streams.get( i ).getTransformMeta() );
    }

    if ( stream == newValidation ) {

      // Add the info..
      //
      Validation validation = new Validation();
      validation.setName( stream.getTransformName() );
      validation.setSourcingTransform( stream.getTransformMeta() );
      validation.setSourcingValues( true );
      validations.add( validation );
    }

    resetTransformIoMeta(); // force transformIo to be recreated when it is next needed.
  }
}
