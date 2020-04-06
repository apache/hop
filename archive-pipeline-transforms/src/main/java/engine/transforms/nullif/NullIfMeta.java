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

package org.apache.hop.pipeline.transforms.nullif;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
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

/*
 * Created on 05-aug-2003
 *
 */

@InjectionSupported( localizationPrefix = "Injection.NullIf.", groups = { "FIELDS" } )
public class NullIfMeta extends BaseTransformMeta implements ITransform {
  private static Class<?> PKG = NullIfMeta.class; // for i18n purposes, needed by Translator!!

  public static class Field implements Cloneable {

    @Injection( name = "FIELDNAME", group = "FIELDS" )
    private String fieldName;
    @Injection( name = "FIELDVALUE", group = "FIELDS" )
    private String fieldValue;

    /**
     * @return Returns the fieldName.
     */
    public String getFieldName() {
      return fieldName;
    }

    /**
     * @param fieldName The fieldName to set.
     */
    public void setFieldName( String fieldName ) {
      this.fieldName = fieldName;
    }

    /**
     * @return Returns the fieldValue.
     */
    public String getFieldValue() {
      return fieldValue;
    }

    /**
     * @param fieldValue The fieldValue to set.
     */
    public void setFieldValue( String fieldValue ) {
      Boolean isEmptyAndNullDiffer = ValueMetaBase.convertStringToBoolean(
        Const.NVL( System.getProperty( Const.HOP_EMPTY_STRING_DIFFERS_FROM_NULL, "N" ), "N" ) );

      this.fieldValue = fieldValue == null && isEmptyAndNullDiffer ? Const.EMPTY_STRING : fieldValue;
    }

    public Field clone() {
      try {
        return (Field) super.clone();
      } catch ( CloneNotSupportedException e ) {
        throw new RuntimeException( e );
      }
    }
  }

  @InjectionDeep
  private Field[] fields;

  public NullIfMeta() {
    super(); // allocate BaseTransformMeta
  }

  public Field[] getFields() {
    return fields;
  }

  public void setFields( Field[] fields ) {
    this.fields = fields;
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
  }

  public void allocate( int count ) {
    fields = new Field[ count ];
    for ( int i = 0; i < count; i++ ) {
      fields[ i ] = new Field();
    }
  }

  public Object clone() {
    NullIfMeta retval = (NullIfMeta) super.clone();

    int count = fields.length;

    retval.allocate( count );

    for ( int i = 0; i < count; i++ ) {
      retval.getFields()[ i ] = fields[ i ].clone();
    }
    return retval;
  }

  private void readData( Node transformNode ) throws HopXMLException {
    try {
      Node fieldNodes = XMLHandler.getSubNode( transformNode, "fields" );
      int count = XMLHandler.countNodes( fieldNodes, "field" );

      allocate( count );

      for ( int i = 0; i < count; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( fieldNodes, "field", i );

        fields[ i ].setFieldName( XMLHandler.getTagValue( fnode, "name" ) );
        fields[ i ].setFieldValue( XMLHandler.getTagValue( fnode, "value" ) );
      }
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString( PKG, "NullIfMeta.Exception.UnableToReadTransformMetaFromXML" ),
        e );
    }
  }

  public void setDefault() {
    int count = 0;

    allocate( count );

    for ( int i = 0; i < count; i++ ) {
      fields[ i ].setFieldName( "field" + i );
      fields[ i ].setFieldValue( "" );
    }
  }

  public void getFields( IRowMeta r, String name, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) {
    if ( r == null ) {
      r = new RowMeta(); // give back values
      // Meta-data doesn't change here, only the value possibly turns to NULL
    }

    return;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "    <fields>" + Const.CR );

    for ( int i = 0; i < fields.length; i++ ) {
      retval.append( "      <field>" + Const.CR );
      retval.append( "        " + XMLHandler.addTagValue( "name", fields[ i ].getFieldName() ) );
      retval.append( "        " + XMLHandler.addTagValue( "value", fields[ i ].getFieldValue() ) );
      retval.append( "        </field>" + Const.CR );
    }
    retval.append( "      </fields>" + Const.CR );

    return retval.toString();
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta, IRowMeta prev,
                     String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString( PKG,
          "NullIfMeta.CheckResult.NoReceivingFieldsError" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
          "NullIfMeta.CheckResult.TransformReceivingFieldsOK", prev.size() + "" ), transformMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
          "NullIfMeta.CheckResult.TransformRecevingInfoFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
          "NullIfMeta.CheckResult.NoInputReceivedError" ), transformMeta );
      remarks.add( cr );
    }
  }

  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr, PipelineMeta pipelineMeta,
                                Pipeline pipeline ) {
    return new NullIf( transformMeta, this, data, cnr, pipelineMeta, pipeline );
  }

  public ITransformData getTransformData() {
    return new NullIfData();
  }

}
