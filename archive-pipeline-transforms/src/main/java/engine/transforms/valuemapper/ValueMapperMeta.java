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

package org.apache.hop.pipeline.transforms.valuemapper;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.injection.AfterInjection;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaString;
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

import java.util.List;

/**
 * Maps String values of a certain field to new values
 * <p>
 * Created on 03-apr-2006
 */
@InjectionSupported( localizationPrefix = "ValueMapper.Injection.", groups = { "VALUES" } )
public class ValueMapperMeta extends BaseTransformMeta implements ITransform {
  private static Class<?> PKG = ValueMapperMeta.class; // for i18n purposes, needed by Translator!!

  @Injection( name = "FIELDNAME" )
  private String fieldToUse;
  @Injection( name = "TARGET_FIELDNAME" )
  private String targetField;
  @Injection( name = "NON_MATCH_DEFAULT" )
  private String nonMatchDefault;

  @Injection( name = "SOURCE", group = "VALUES" )
  private String[] sourceValue;
  @Injection( name = "TARGET", group = "VALUES" )
  private String[] targetValue;

  public ValueMapperMeta() {
    super(); // allocate BaseTransformMeta
  }

  /**
   * @return Returns the fieldName.
   */
  public String[] getSourceValue() {
    return sourceValue;
  }

  /**
   * @param fieldName The fieldName to set.
   */
  public void setSourceValue( String[] fieldName ) {
    this.sourceValue = fieldName;
  }

  /**
   * @return Returns the fieldValue.
   */
  public String[] getTargetValue() {
    return targetValue;
  }

  /**
   * @param fieldValue The fieldValue to set.
   */
  public void setTargetValue( String[] fieldValue ) {
    this.targetValue = fieldValue;
  }

  @Override
  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
  }

  public void allocate( int count ) {
    sourceValue = new String[ count ];
    targetValue = new String[ count ];
  }

  @Override
  public Object clone() {
    ValueMapperMeta retval = (ValueMapperMeta) super.clone();

    int count = sourceValue.length;

    retval.allocate( count );

    System.arraycopy( sourceValue, 0, retval.sourceValue, 0, count );
    System.arraycopy( targetValue, 0, retval.targetValue, 0, count );

    return retval;
  }

  private void readData( Node transformNode ) throws HopXMLException {
    try {
      fieldToUse = XMLHandler.getTagValue( transformNode, "field_to_use" );
      targetField = XMLHandler.getTagValue( transformNode, "target_field" );
      nonMatchDefault = XMLHandler.getTagValue( transformNode, "non_match_default" );

      Node fields = XMLHandler.getSubNode( transformNode, "fields" );
      int count = XMLHandler.countNodes( fields, "field" );

      allocate( count );

      for ( int i = 0; i < count; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fields, "field", i );

        sourceValue[ i ] = XMLHandler.getTagValue( fnode, "source_value" );
        targetValue[ i ] = XMLHandler.getTagValue( fnode, "target_value" );
      }
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "ValueMapperMeta.RuntimeError.UnableToReadXML.VALUEMAPPER0004" ), e );
    }
  }

  @Override
  public void setDefault() {
    int count = 0;

    allocate( count );

    for ( int i = 0; i < count; i++ ) {
      sourceValue[ i ] = "field" + i;
      targetValue[ i ] = "";
    }
  }

  @Override
  public void getFields( IRowMeta r, String name, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) {
    IValueMeta extra = null;
    if ( !Utils.isEmpty( getTargetField() ) ) {
      extra = new ValueMetaString( getTargetField() );

      // Lengths etc?
      // Take the max length of all the strings...
      //
      int maxlen = -1;
      for ( int i = 0; i < targetValue.length; i++ ) {
        if ( targetValue[ i ] != null && targetValue[ i ].length() > maxlen ) {
          maxlen = targetValue[ i ].length();
        }
      }

      // include default value in max length calculation
      //
      if ( nonMatchDefault != null && nonMatchDefault.length() > maxlen ) {
        maxlen = nonMatchDefault.length();
      }
      extra.setLength( maxlen );
      extra.setOrigin( name );
      r.addValueMeta( extra );
    } else {
      if ( !Utils.isEmpty( getFieldToUse() ) ) {
        extra = r.searchValueMeta( getFieldToUse() );
      }
    }

    if ( extra != null ) {
      // The output of a changed field or new field is always a normal storage type...
      //
      extra.setStorageType( IValueMeta.STORAGE_TYPE_NORMAL );
    }
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "    " ).append( XMLHandler.addTagValue( "field_to_use", fieldToUse ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "target_field", targetField ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "non_match_default", nonMatchDefault ) );

    retval.append( "    <fields>" ).append( Const.CR );

    for ( int i = 0; i < sourceValue.length; i++ ) {
      retval.append( "      <field>" ).append( Const.CR );
      retval.append( "        " ).append( XMLHandler.addTagValue( "source_value", sourceValue[ i ] ) );
      retval.append( "        " ).append( XMLHandler.addTagValue( "target_value", targetValue[ i ] ) );
      retval.append( "      </field>" ).append( Const.CR );
    }
    retval.append( "    </fields>" ).append( Const.CR );

    return retval.toString();
  }

  private String getNullOrEmpty( String str ) {
    return str == null ? StringUtils.EMPTY : str;
  }

  @Override
  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "ValueMapperMeta.CheckResult.NotReceivingFieldsFromPreviousTransforms" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "ValueMapperMeta.CheckResult.ReceivingFieldsFromPreviousTransforms", "" + prev.size() ), transformMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "ValueMapperMeta.CheckResult.ReceivingInfoFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "ValueMapperMeta.CheckResult.NotReceivingInfoFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    }
  }

  @Override
  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new ValueMapper( transformMeta, this, data, cnr, pipelineMeta, pipeline );
  }

  @Override
  public ITransformData getTransformData() {
    return new ValueMapperData();
  }

  /**
   * @return Returns the fieldToUse.
   */
  public String getFieldToUse() {
    return fieldToUse;
  }

  /**
   * @param fieldToUse The fieldToUse to set.
   */
  public void setFieldToUse( String fieldToUse ) {
    this.fieldToUse = fieldToUse;
  }

  /**
   * @return Returns the targetField.
   */
  public String getTargetField() {
    return targetField;
  }

  /**
   * @param targetField The targetField to set.
   */
  public void setTargetField( String targetField ) {
    this.targetField = targetField;
  }

  /**
   * @return the non match default. This is the string that will be used to fill in the data when no match is found.
   */
  public String getNonMatchDefault() {
    return nonMatchDefault;
  }

  /**
   * @param nonMatchDefault the non match default. This is the string that will be used to fill in the data when no match is found.
   */
  public void setNonMatchDefault( String nonMatchDefault ) {
    this.nonMatchDefault = nonMatchDefault;
  }

  /**
   * If we use injection we can have different arrays lengths.
   * We need synchronize them for consistency behavior with UI
   */
  @AfterInjection
  public void afterInjectionSynchronization() {
    int nrFields = ( sourceValue == null ) ? -1 : sourceValue.length;
    if ( nrFields <= 0 ) {
      return;
    }
    String[][] rtn = Utils.normalizeArrays( nrFields, targetValue );
    targetValue = rtn[ 0 ];
  }

}
