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

package org.apache.hop.pipeline.transforms.xml.xmljoin;

import java.util.Arrays;
import java.util.List;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.Const;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;

import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.w3c.dom.Node;

/**
 * This class knows how to handle the MetaData for the XML join step
 * 
 * @since 30-04-2008
 * 
 */
@Transform(
        id = "XMLJoin",
        image = "XJN.svg",
        i18nPackageName = "org.apache.hop.pipeline.transforms.xml.xmljoin",
        name = "XMLJoin.name",
        description = "XMLJoin.description",
        categoryDescription = "XMLJoin.category",
        documentationUrl = "https://www.project-hop.org/manual/latest/plugins/transforms/xmljoin.html"
)
@InjectionSupported( localizationPrefix = "XMLJoin.Injection." )
public class XmlJoinMeta extends BaseTransformMeta implements ITransformMeta<XmlJoin, XmlJoinData> {
  private static Class<?> PKG = XmlJoinMeta.class; // for i18n purposes, needed by Translator2!!

  /** The base name of the output file */

  /** Flag: execute complex join */
  @Injection( name = "COMPLEX_JOIN" )
  private boolean complexJoin;

  /** What step holds the xml string to join into */
  @Injection( name = "TARGET_XML_STEP" )
  private String targetXMLstep;

  /** What field holds the xml string to join into */
  @Injection( name = "TARGET_XML_FIELD" )
  private String targetXMLfield;

  /** What field holds the XML tags to join */
  @Injection( name = "SOURCE_XML_FIELD" )
  private String sourceXMLfield;

  /** The name value containing the resulting XML fragment */
  @Injection( name = "VALUE_XML_FIELD" )
  private String valueXMLfield;

  /** The name of the repeating row XML element */
  @Injection( name = "TARGET_XPATH" )
  private String targetXPath;

  /** What step holds the xml strings to join */
  @Injection( name = "SOURCE_XML_STEP" )
  private String sourceXMLstep;

  /** What field holds the join compare value */
  @Injection( name = "JOIN_COMPARE_FIELD" )
  private String joinCompareField;

  /** The encoding to use for reading: null or empty string means system default encoding */
  @Injection( name = "ENCODING" )
  private String encoding;

  /** Flag: execute complex join */
  @Injection( name = "OMIT_XML_HEADER" )
  private boolean omitXMLHeader;

  /** Flag: omit null values from result xml */
  @Injection( name = "OMIT_NULL_VALUES" )
  private boolean omitNullValues;

  public XmlJoinMeta() {
    super(); // allocate BaseStepMeta
  }

  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXmlException {
    readData( stepnode );
  }

  public Object clone() {
    XmlJoinMeta retval = (XmlJoinMeta) super.clone();
    return retval;
  }

  private void readData( Node stepnode ) throws HopXmlException {
    try {
      valueXMLfield = XmlHandler.getTagValue( stepnode, "valueXMLfield" );
      targetXMLstep = XmlHandler.getTagValue( stepnode, "targetXMLstep" );
      targetXMLfield = XmlHandler.getTagValue( stepnode, "targetXMLfield" );
      sourceXMLstep = XmlHandler.getTagValue( stepnode, "sourceXMLstep" );
      sourceXMLfield = XmlHandler.getTagValue( stepnode, "sourceXMLfield" );
      targetXPath = XmlHandler.getTagValue( stepnode, "targetXPath" );
      joinCompareField = XmlHandler.getTagValue( stepnode, "joinCompareField" );
      encoding = XmlHandler.getTagValue( stepnode, "encoding" );
      complexJoin = "Y".equalsIgnoreCase( XmlHandler.getTagValue( stepnode, "complexJoin" ) );
      omitXMLHeader = "Y".equalsIgnoreCase( XmlHandler.getTagValue( stepnode, "omitXMLHeader" ) );
      omitNullValues = "Y".equalsIgnoreCase( XmlHandler.getTagValue( stepnode, "omitNullValues" ) );

    } catch ( Exception e ) {
      throw new HopXmlException( "Unable to load step info from XML", e );
    }
  }

  public void setDefault() {
    // complexJoin = false;
    encoding = Const.XML_ENCODING;
  }

  public void getFields(IRowMeta row, String name, IRowMeta[] info, TransformMeta nextStep,
                        IVariables space, IMetaStore metaStore ) throws HopTransformException {

    IValueMeta v = new ValueMetaString( this.getValueXmlField());
    v.setOrigin( name );

    PipelineMeta transMeta = (PipelineMeta) space;
    try {
      // Row should only include fields from the target and not the source. During the preview table generation
      // the fields from all previous steps (source and target) are included in the row so lets remove the
      // source fields.
      List<String> targetFieldNames = null;
      IRowMeta targetRowMeta = transMeta.getTransformFields( transMeta.findTransform( getTargetXmlStep() ),
        null,
        null );
      if ( targetRowMeta != null ) {
        targetFieldNames = Arrays.asList( targetRowMeta.getFieldNames() );
      }
      for ( String fieldName : transMeta.getTransformFields( transMeta.findTransform( getSourceXmlStep() ),
                                                                            null,
                                                                            null ).getFieldNames() ) {
        if ( targetFieldNames == null || !targetFieldNames.contains( fieldName ) ) {
          row.removeValueMeta( fieldName );
        }
      }
    } catch ( HopValueException e ) {
      // Pass
    }

    row.addValueMeta( v );
  }

  public String getXml() {
    StringBuffer retval = new StringBuffer( 500 );

    retval.append( "    " ).append( XmlHandler.addTagValue( "valueXMLField", valueXMLfield ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "targetXMLstep", targetXMLstep ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "targetXMLfield", targetXMLfield ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "sourceXMLstep", sourceXMLstep ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "sourceXMLfield", sourceXMLfield ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "complexJoin", complexJoin ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "joinCompareField", joinCompareField ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "targetXPath", targetXPath ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "encoding", encoding ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "omitXMLHeader", omitXMLHeader ) );
    retval.append( "    " ).append( XmlHandler.addTagValue( "omitNullValues", omitNullValues ) );

    return retval.toString();
  }


  public void check(List<ICheckResult> remarks, PipelineMeta transMeta, TransformMeta stepMeta, IRowMeta prev,
                    String[] input, String[] output, IRowMeta info, IVariables space, IMetaStore metaStore ) {

    CheckResult cr;
    // checks for empty field which are required
    if ( this.targetXMLstep == null || this.targetXMLstep.length() == 0 ) {
      cr =
          new CheckResult( ICheckResult.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
              "XMLJoin.CheckResult.TargetXMLStepNotSpecified" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
          new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString( PKG,
              "XMLJoin.CheckResult.TargetXMLStepSpecified" ), stepMeta );
      remarks.add( cr );
    }
    if ( this.targetXMLfield == null || this.targetXMLfield.length() == 0 ) {
      cr =
          new CheckResult( ICheckResult.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
              "XMLJoin.CheckResult.TargetXMLFieldNotSpecified" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
          new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString( PKG,
              "XMLJoin.CheckResult.TargetXMLFieldSpecified" ), stepMeta );
      remarks.add( cr );
    }
    if ( this.sourceXMLstep == null || this.sourceXMLstep.length() == 0 ) {
      cr =
          new CheckResult( ICheckResult.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
              "XMLJoin.CheckResult.SourceXMLStepNotSpecified" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
          new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString( PKG,
              "XMLJoin.CheckResult.SourceXMLStepSpecified" ), stepMeta );
      remarks.add( cr );
    }
    if ( this.sourceXMLfield == null || this.sourceXMLfield.length() == 0 ) {
      cr =
          new CheckResult( ICheckResult.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
              "XMLJoin.CheckResult.SourceXMLFieldNotSpecified" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
          new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString( PKG,
              "XMLJoin.CheckResult.SourceXMLFieldSpecified" ), stepMeta );
      remarks.add( cr );
    }
    if ( this.valueXMLfield == null || this.valueXMLfield.length() == 0 ) {
      cr =
          new CheckResult( ICheckResult.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
              "XMLJoin.CheckResult.ResultFieldNotSpecified" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
          new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString( PKG,
              "XMLJoin.CheckResult.ResultFieldSpecified" ), stepMeta );
      remarks.add( cr );
    }
    if ( this.targetXPath == null || this.targetXPath.length() == 0 ) {
      cr =
          new CheckResult( ICheckResult.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
              "XMLJoin.CheckResult.TargetXPathNotSpecified" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
          new CheckResult( ICheckResult.TYPE_RESULT_OK, BaseMessages.getString( PKG,
              "XMLJoin.CheckResult.TargetXPathSpecified" ), stepMeta );
      remarks.add( cr );
    }

    // See if we have the right input streams leading to this step!
    if ( input.length > 0 ) {
      boolean targetStepFound = false;
      boolean sourceStepFound = false;
      for ( int i = 0; i < input.length; i++ ) {
        if ( this.targetXMLstep != null && this.targetXMLstep.equals( input[i] ) ) {
          targetStepFound = true;
          cr =
              new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString( PKG,
                  "XMLJoin.CheckResult.TargetXMLStepFound", this.targetXMLstep ), stepMeta );
          remarks.add( cr );
        }
        if ( this.sourceXMLstep != null && this.sourceXMLstep.equals( input[i] ) ) {
          sourceStepFound = true;
          cr =
              new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString( PKG,
                  "XMLJoin.CheckResult.SourceXMLStepFound", this.sourceXMLstep ), stepMeta );
          remarks.add( cr );
        }
      }

      if ( !targetStepFound ) {
        cr =
            new CheckResult( ICheckResult.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
                "XMLJoin.CheckResult.TargetXMLStepNotFound", this.targetXMLstep ), stepMeta );
        remarks.add( cr );
      }
      if ( !sourceStepFound ) {
        cr =
            new CheckResult( ICheckResult.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
                "XMLJoin.CheckResult.SourceXMLStepNotFound", this.sourceXMLstep ), stepMeta );
        remarks.add( cr );
      }
    } else {
      cr =
          new CheckResult( ICheckResult.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
              "XMLJoin.CheckResult.ExpectedInputError" ), stepMeta );
      remarks.add( cr );
    }
  }

  public boolean isComplexJoin() {
    return complexJoin;
  }

  public void setComplexJoin( boolean complexJoin ) {
    this.complexJoin = complexJoin;
  }

  public String getTargetXmlStep() {
    return targetXMLstep;
  }

  public void setTargetXmlStep(String targetXMLstep ) {
    this.targetXMLstep = targetXMLstep;
  }

  public String getTargetXmlField() {
    return targetXMLfield;
  }

  public void setTargetXmlField(String targetXMLfield ) {
    this.targetXMLfield = targetXMLfield;
  }

  public String getSourceXmlStep() {
    return sourceXMLstep;
  }

  public void setSourceXmlStep(String targetXMLstep ) {
    this.sourceXMLstep = targetXMLstep;
  }

  public String getSourceXmlField() {
    return sourceXMLfield;
  }

  public void setSourceXmlField(String sourceXMLfield ) {
    this.sourceXMLfield = sourceXMLfield;
  }

  public String getValueXmlField() {
    return valueXMLfield;
  }

  public void setValueXmlField(String valueXMLfield ) {
    this.valueXMLfield = valueXMLfield;
  }

  public String getTargetXPath() {
    return targetXPath;
  }

  public void setTargetXPath( String targetXPath ) {
    this.targetXPath = targetXPath;
  }

  public String getJoinCompareField() {
    return joinCompareField;
  }

  public void setJoinCompareField( String joinCompareField ) {
    this.joinCompareField = joinCompareField;
  }

  public boolean excludeFromRowLayoutVerification() {
    return true;
  }

  @Override
  public ITransform createTransform(TransformMeta transformMeta, XmlJoinData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline) {
    return new XmlJoin(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public XmlJoinData getTransformData() {
    return new XmlJoinData();
  }

  public boolean isOmitXmlHeader() {
    return omitXMLHeader;
  }

  public void setOmitXmlHeader(boolean omitXMLHeader ) {
    this.omitXMLHeader = omitXMLHeader;
  }

  public void setOmitNullValues( boolean omitNullValues ) {

    this.omitNullValues = omitNullValues;

  }

  public boolean isOmitNullValues() {

    return omitNullValues;

  }

  public String getEncoding() {
    return encoding;
  }

  public void setEncoding( String encoding ) {
    this.encoding = encoding;
  }

}
