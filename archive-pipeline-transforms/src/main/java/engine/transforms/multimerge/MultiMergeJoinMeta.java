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

package org.apache.hop.pipeline.transforms.multimerge;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.injection.Injection;
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
import org.apache.hop.pipeline.transform.TransformIOMetaInterface;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.apache.hop.pipeline.transform.errorhandling.StreamIcon;
import org.apache.hop.pipeline.transform.errorhandling.StreamInterface.StreamType;
import org.w3c.dom.Node;

import java.util.List;

/**
 * @author Biswapesh
 * @since 24-nov-2006
 */
@InjectionSupported( localizationPrefix = "MultiMergeJoin.Injection." )
public class MultiMergeJoinMeta extends BaseTransformMeta implements ITransform {
  private static Class<?> PKG = MultiMergeJoinMeta.class; // for i18n purposes, needed by Translator!!

  public static final String[] join_types = { "INNER", "FULL OUTER" };
  public static final boolean[] optionals = { false, true };

  @Injection( name = "JOIN_TYPE" )
  private String joinType;

  /**
   * comma separated key values for each stream
   */
  @Injection( name = "KEY_FIELDS" )
  private String[] keyFields;

  /**
   * input stream names
   */
  @Injection( name = "INPUT_TRANSFORMS" )
  private String[] inputTransforms;

  /**
   * The supported join types are INNER, LEFT OUTER, RIGHT OUTER and FULL OUTER
   *
   * @return The type of join
   */
  public String getJoinType() {
    return joinType;
  }

  /**
   * Sets the type of join
   *
   * @param joinType The type of join, e.g. INNER/FULL OUTER
   */
  public void setJoinType( String joinType ) {
    this.joinType = joinType;
  }

  /**
   * @return Returns the keyFields1.
   */
  public String[] getKeyFields() {
    return keyFields;
  }

  /**
   * @param keyFields1 The keyFields1 to set.
   */
  public void setKeyFields( String[] keyFields ) {
    this.keyFields = keyFields;
  }

  @Override
  public boolean excludeFromRowLayoutVerification() {
    return true;
  }

  public MultiMergeJoinMeta() {
    super(); // allocate BaseTransformMeta
  }

  @Override
  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
  }

  public void allocateKeys( int nrKeys ) {
    keyFields = new String[ nrKeys ];
  }

  @Override
  public Object clone() {
    MultiMergeJoinMeta retval = (MultiMergeJoinMeta) super.clone();
    int nrKeys = keyFields == null ? 0 : keyFields.length;
    int nrTransforms = inputTransforms == null ? 0 : inputTransforms.length;
    retval.allocateKeys( nrKeys );
    retval.allocateInputTransforms( nrTransforms );
    System.arraycopy( keyFields, 0, retval.keyFields, 0, nrKeys );
    System.arraycopy( inputTransforms, 0, retval.inputTransforms, 0, nrTransforms );
    return retval;
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder();

    String[] inputTransformsNames = inputTransforms != null ? inputTransforms : ArrayUtils.EMPTY_STRING_ARRAY;
    retval.append( "    " ).append( XMLHandler.addTagValue( "join_type", getJoinType() ) );
    for ( int i = 0; i < inputTransformsNames.length; i++ ) {
      retval.append( "    " ).append( XMLHandler.addTagValue( "transform" + i, inputTransformsNames[ i ] ) );
    }

    retval.append( "    " ).append( XMLHandler.addTagValue( "number_input", inputTransformsNames.length ) );
    retval.append( "    " ).append( XMLHandler.openTag( "keys" ) ).append( Const.CR );
    for ( int i = 0; i < keyFields.length; i++ ) {
      retval.append( "      " ).append( XMLHandler.addTagValue( "key", keyFields[ i ] ) );
    }
    retval.append( "    " ).append( XMLHandler.closeTag( "keys" ) ).append( Const.CR );

    return retval.toString();
  }

  private void readData( Node transformNode ) throws HopXMLException {
    try {

      Node keysNode = XMLHandler.getSubNode( transformNode, "keys" );

      int nrKeys = XMLHandler.countNodes( keysNode, "key" );

      allocateKeys( nrKeys );

      for ( int i = 0; i < nrKeys; i++ ) {
        Node keynode = XMLHandler.getSubNodeByNr( keysNode, "key", i );
        keyFields[ i ] = XMLHandler.getNodeValue( keynode );
      }

      int nInputStreams = Integer.parseInt( XMLHandler.getTagValue( transformNode, "number_input" ) );

      allocateInputTransforms( nInputStreams );

      for ( int i = 0; i < nInputStreams; i++ ) {
        inputTransforms[ i ] = XMLHandler.getTagValue( transformNode, "transform" + i );
      }

      joinType = XMLHandler.getTagValue( transformNode, "join_type" );
    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString( PKG, "MultiMergeJoinMeta.Exception.UnableToLoadTransformMeta" ),
        e );
    }
  }

  @Override
  public void setDefault() {
    joinType = join_types[ 0 ];
    allocateKeys( 0 );
    allocateInputTransforms( 0 );
  }

  @Override
  public void searchInfoAndTargetTransforms( List<TransformMeta> transforms ) {
    TransformIOMetaInterface ioMeta = getTransformIOMeta();
    ioMeta.getInfoStreams().clear();
    for ( int i = 0; i < inputTransforms.length; i++ ) {
      String inputTransformName = inputTransforms[ i ];
      if ( i >= ioMeta.getInfoStreams().size() ) {
        ioMeta.addStream(
          new Stream( StreamType.INFO, TransformMeta.findTransform( transforms, inputTransformName ),
            BaseMessages.getString( PKG, "MultiMergeJoin.InfoStream.Description" ), StreamIcon.INFO, inputTransformName ) );
      }
    }
  }

  @Override
  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta, IRowMeta prev,
                     String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    /*
     * @todo Need to check for the following: 1) Join type must be one of INNER / LEFT OUTER / RIGHT OUTER / FULL OUTER
     * 2) Number of input streams must be two (for now at least) 3) The field names of input streams must be unique
     */
    CheckResult cr =
      new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString( PKG,
        "MultiMergeJoinMeta.CheckResult.TransformNotVerified" ), transformMeta );
    remarks.add( cr );
  }

  @Override
  public void getFields( IRowMeta r, String name, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {
    // We don't have any input fields here in "r" as they are all info fields.
    // So we just merge in the info fields.
    //
    if ( info != null ) {
      for ( int i = 0; i < info.length; i++ ) {
        if ( info[ i ] != null ) {
          r.mergeRowMeta( info[ i ] );
        }
      }
    }

    for ( int i = 0; i < r.size(); i++ ) {
      r.getValueMeta( i ).setOrigin( name );
    }
    return;
  }

  @Override
  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new MultiMergeJoin( transformMeta, this, data, cnr, tr, pipeline );
  }

  @Override
  public ITransformData getTransformData() {
    return new MultiMergeJoinData();
  }

  @Override
  public void resetTransformIoMeta() {
    // Don't reset!
  }

  public void setInputTransforms( String[] inputTransforms ) {
    this.inputTransforms = inputTransforms;
  }

  public String[] getInputTransforms() {
    return inputTransforms;
  }

  public void allocateInputTransforms( int count ) {
    inputTransforms = new String[ count ];

  }
}
