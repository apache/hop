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

package org.apache.hop.pipeline.transforms.rowsfromresult;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
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
 * Created on 02-jun-2003
 *
 */

public class RowsFromResultMeta extends BaseTransformMeta implements ITransform {
  private static Class<?> PKG = RowsFromResult.class; // for i18n purposes, needed by Translator!!

  private String[] fieldname;
  private int[] type;
  private int[] length;
  private int[] precision;

  /**
   * @return Returns the length.
   */
  public int[] getLength() {
    return length;
  }

  /**
   * @param length The length to set.
   */
  public void setLength( int[] length ) {
    this.length = length;
  }

  /**
   * @return Returns the name.
   */
  public String[] getFieldname() {
    return fieldname;
  }

  /**
   * @param name The name to set.
   */
  public void setFieldname( String[] name ) {
    this.fieldname = name;
  }

  /**
   * @return Returns the precision.
   */
  public int[] getPrecision() {
    return precision;
  }

  /**
   * @param precision The precision to set.
   */
  public void setPrecision( int[] precision ) {
    this.precision = precision;
  }

  /**
   * @return Returns the type.
   */
  public int[] getType() {
    return type;
  }

  /**
   * @param type The type to set.
   */
  public void setType( int[] type ) {
    this.type = type;
  }

  public RowsFromResultMeta() {
    super(); // allocate BaseTransformMeta
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
  }

  public Object clone() {
    RowsFromResultMeta retval = (RowsFromResultMeta) super.clone();
    int nrFields = fieldname.length;
    retval.allocate( nrFields );
    System.arraycopy( fieldname, 0, retval.fieldname, 0, nrFields );
    System.arraycopy( type, 0, retval.type, 0, nrFields );
    System.arraycopy( length, 0, retval.length, 0, nrFields );
    System.arraycopy( precision, 0, retval.precision, 0, nrFields );
    return retval;
  }

  public void allocate( int nrFields ) {
    fieldname = new String[ nrFields ];
    type = new int[ nrFields ];
    length = new int[ nrFields ];
    precision = new int[ nrFields ];
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();
    retval.append( "    <fields>" );
    for ( int i = 0; i < fieldname.length; i++ ) {
      retval.append( "      <field>" );
      retval.append( "        " + XMLHandler.addTagValue( "name", fieldname[ i ] ) );
      retval.append( "        " + XMLHandler.addTagValue( "type", ValueMetaFactory.getValueMetaName( type[ i ] ) ) );
      retval.append( "        " + XMLHandler.addTagValue( "length", length[ i ] ) );
      retval.append( "        " + XMLHandler.addTagValue( "precision", precision[ i ] ) );
      retval.append( "        </field>" );
    }
    retval.append( "      </fields>" );

    return retval.toString();
  }

  private void readData( Node transformNode ) {
    Node fields = XMLHandler.getSubNode( transformNode, "fields" );
    int nrFields = XMLHandler.countNodes( fields, "field" );

    allocate( nrFields );

    for ( int i = 0; i < nrFields; i++ ) {
      Node line = XMLHandler.getSubNodeByNr( fields, "field", i );
      fieldname[ i ] = XMLHandler.getTagValue( line, "name" );
      type[ i ] = ValueMetaFactory.getIdForValueMeta( XMLHandler.getTagValue( line, "type" ) );
      length[ i ] = Const.toInt( XMLHandler.getTagValue( line, "length" ), -2 );
      precision[ i ] = Const.toInt( XMLHandler.getTagValue( line, "precision" ), -2 );
    }

  }

  public void setDefault() {
    allocate( 0 );
  }

  public void getFields( IRowMeta r, String origin, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {
    for ( int i = 0; i < this.fieldname.length; i++ ) {
      IValueMeta v;
      try {
        v = ValueMetaFactory.createValueMeta( fieldname[ i ], type[ i ], length[ i ], precision[ i ] );
        v.setOrigin( origin );
        r.addValueMeta( v );
      } catch ( HopPluginException e ) {
        throw new HopTransformException( e );
      }
    }
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      CheckResult cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "RowsFromResultMeta.CheckResult.TransformExpectingNoReadingInfoFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    } else {
      CheckResult cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "RowsFromResultMeta.CheckResult.NoInputReceivedError" ), transformMeta );
      remarks.add( cr );
    }
  }

  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new RowsFromResult( transformMeta, this, data, cnr, pipelineMeta, pipeline );
  }

  public ITransformData getTransformData() {
    return new RowsFromResultData();
  }

}
