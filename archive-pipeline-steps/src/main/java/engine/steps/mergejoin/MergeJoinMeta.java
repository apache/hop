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

package org.apache.hop.pipeline.steps.mergejoin;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.row.ValueMetaInterface;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.TransformationType;
import org.apache.hop.pipeline.step.BaseStepMeta;
import org.apache.hop.pipeline.step.StepDataInterface;
import org.apache.hop.pipeline.step.StepIOMeta;
import org.apache.hop.pipeline.step.StepIOMetaInterface;
import org.apache.hop.pipeline.step.StepInterface;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.step.StepMetaInterface;
import org.apache.hop.pipeline.step.errorhandling.Stream;
import org.apache.hop.pipeline.step.errorhandling.StreamIcon;
import org.apache.hop.pipeline.step.errorhandling.StreamInterface;
import org.apache.hop.pipeline.step.errorhandling.StreamInterface.StreamType;
import org.w3c.dom.Node;

import java.util.List;

/*
 * @author Biswapesh
 * @since 24-nov-2006
 */
@InjectionSupported( localizationPrefix = "MergeJoin.Injection." )
public class MergeJoinMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = MergeJoinMeta.class; // for i18n purposes, needed by Translator!!

  public static final String[] join_types = { "INNER", "LEFT OUTER", "RIGHT OUTER", "FULL OUTER" };
  public static final boolean[] one_optionals = { false, false, true, true };
  public static final boolean[] two_optionals = { false, true, false, true };

  @Injection( name = "JOIN_TYPE" )
  private String joinType;

  @Injection( name = "KEY_FIELD1" )
  private String[] keyFields1;
  @Injection( name = "KEY_FIELD2" )
  private String[] keyFields2;

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
  public String[] getKeyFields1() {
    return keyFields1;
  }

  /**
   * @param keyFields1 The keyFields1 to set.
   */
  public void setKeyFields1( String[] keyFields1 ) {
    this.keyFields1 = keyFields1;
  }

  /**
   * @return Returns the keyFields2.
   */
  public String[] getKeyFields2() {
    return keyFields2;
  }

  /**
   * @param keyFields2 The keyFields2 to set.
   */
  public void setKeyFields2( String[] keyFields2 ) {
    this.keyFields2 = keyFields2;
  }

  public boolean excludeFromRowLayoutVerification() {
    return true;
  }

  public MergeJoinMeta() {
    super(); // allocate BaseStepMeta
  }

  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode );
  }

  public void allocate( int nrKeys1, int nrKeys2 ) {
    keyFields1 = new String[ nrKeys1 ];
    keyFields2 = new String[ nrKeys2 ];
  }

  public Object clone() {
    MergeJoinMeta retval = (MergeJoinMeta) super.clone();
    int nrKeys1 = keyFields1.length;
    int nrKeys2 = keyFields2.length;
    retval.allocate( nrKeys1, nrKeys2 );
    System.arraycopy( keyFields1, 0, retval.keyFields1, 0, nrKeys1 );
    System.arraycopy( keyFields2, 0, retval.keyFields2, 0, nrKeys2 );

    StepIOMetaInterface stepIOMeta = new StepIOMeta( true, true, false, false, false, false );
    List<StreamInterface> infoStreams = getStepIOMeta().getInfoStreams();

    for ( StreamInterface infoStream : infoStreams ) {
      stepIOMeta.addStream( new Stream( infoStream ) );
    }
    retval.setStepIOMeta( stepIOMeta );

    return retval;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    List<StreamInterface> infoStreams = getStepIOMeta().getInfoStreams();

    retval.append( XMLHandler.addTagValue( "join_type", getJoinType() ) );
    retval.append( XMLHandler.addTagValue( "step1", infoStreams.get( 0 ).getStepname() ) );
    retval.append( XMLHandler.addTagValue( "step2", infoStreams.get( 1 ).getStepname() ) );

    retval.append( "    <keys_1>" + Const.CR );
    for ( int i = 0; i < keyFields1.length; i++ ) {
      retval.append( "      " + XMLHandler.addTagValue( "key", keyFields1[ i ] ) );
    }
    retval.append( "    </keys_1>" + Const.CR );

    retval.append( "    <keys_2>" + Const.CR );
    for ( int i = 0; i < keyFields2.length; i++ ) {
      retval.append( "      " + XMLHandler.addTagValue( "key", keyFields2[ i ] ) );
    }
    retval.append( "    </keys_2>" + Const.CR );

    return retval.toString();
  }

  private void readData( Node stepnode ) throws HopXMLException {
    try {

      Node keysNode1 = XMLHandler.getSubNode( stepnode, "keys_1" );
      Node keysNode2 = XMLHandler.getSubNode( stepnode, "keys_2" );

      int nrKeys1 = XMLHandler.countNodes( keysNode1, "key" );
      int nrKeys2 = XMLHandler.countNodes( keysNode2, "key" );

      allocate( nrKeys1, nrKeys2 );

      for ( int i = 0; i < nrKeys1; i++ ) {
        Node keynode = XMLHandler.getSubNodeByNr( keysNode1, "key", i );
        keyFields1[ i ] = XMLHandler.getNodeValue( keynode );
      }

      for ( int i = 0; i < nrKeys2; i++ ) {
        Node keynode = XMLHandler.getSubNodeByNr( keysNode2, "key", i );
        keyFields2[ i ] = XMLHandler.getNodeValue( keynode );
      }

      List<StreamInterface> infoStreams = getStepIOMeta().getInfoStreams();
      infoStreams.get( 0 ).setSubject( XMLHandler.getTagValue( stepnode, "step1" ) );
      infoStreams.get( 1 ).setSubject( XMLHandler.getTagValue( stepnode, "step2" ) );
      joinType = XMLHandler.getTagValue( stepnode, "join_type" );
    } catch ( Exception e ) {
      throw new HopXMLException(
        BaseMessages.getString( PKG, "MergeJoinMeta.Exception.UnableToLoadStepInfo" ), e );
    }
  }

  public void setDefault() {
    joinType = join_types[ 0 ];
    allocate( 0, 0 );
  }

  @Override
  public void searchInfoAndTargetSteps( List<StepMeta> steps ) {
    List<StreamInterface> infoStreams = getStepIOMeta().getInfoStreams();
    for ( StreamInterface stream : infoStreams ) {
      stream.setStepMeta( StepMeta.findStep( steps, (String) stream.getSubject() ) );
    }
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, StepMeta stepMeta,
                     RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace space,
                     IMetaStore metaStore ) {
    /*
     * @todo Need to check for the following: 1) Join type must be one of INNER / LEFT OUTER / RIGHT OUTER / FULL OUTER
     * 2) Number of input streams must be two (for now at least) 3) The field names of input streams must be unique
     */
    CheckResult cr =
      new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString(
        PKG, "MergeJoinMeta.CheckResult.StepNotVerified" ), stepMeta );
    remarks.add( cr );
  }

  public void getFields( RowMetaInterface r, String name, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, IMetaStore metaStore ) throws HopStepException {
    // We don't have any input fields here in "r" as they are all info fields.
    // So we just merge in the info fields.
    //
    if ( info != null ) {
      for ( int i = 0; i < info.length; i++ ) {
        if ( info[ i ] != null ) {
          r.mergeRowMeta( info[ i ], name );
        }
      }
    }

    for ( int i = 0; i < r.size(); i++ ) {
      ValueMetaInterface vmi = r.getValueMeta( i );
      if ( vmi != null && Utils.isEmpty( vmi.getName() ) ) {
        vmi.setOrigin( name );
      }
    }
    return;
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new MergeJoin( stepMeta, stepDataInterface, cnr, tr, pipeline );
  }

  public StepDataInterface getStepData() {
    return new MergeJoinData();
  }

  /**
   * Returns the Input/Output metadata for this step. The generator step only produces output, does not accept input!
   */
  public StepIOMetaInterface getStepIOMeta() {
    StepIOMetaInterface ioMeta = super.getStepIOMeta( false );
    if ( ioMeta == null ) {

      ioMeta = new StepIOMeta( true, true, false, false, false, false );

      ioMeta.addStream( new Stream( StreamType.INFO, null, BaseMessages.getString(
        PKG, "MergeJoinMeta.InfoStream.FirstStream.Description" ), StreamIcon.INFO, null ) );
      ioMeta.addStream( new Stream( StreamType.INFO, null, BaseMessages.getString(
        PKG, "MergeJoinMeta.InfoStream.SecondStream.Description" ), StreamIcon.INFO, null ) );
      setStepIOMeta( ioMeta );
    }

    return ioMeta;
  }

  public void resetStepIoMeta() {
    // Don't reset!
  }

  public TransformationType[] getSupportedTransformationTypes() {
    return new TransformationType[] { TransformationType.Normal, };
  }
}
