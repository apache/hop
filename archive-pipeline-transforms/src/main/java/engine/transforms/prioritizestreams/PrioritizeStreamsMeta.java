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

package org.apache.hop.pipeline.transforms.prioritizestreams;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.IRowMeta;
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
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.w3c.dom.Node;

import java.util.List;

/*
 * Created on 30-06-2008
 *
 */

public class PrioritizeStreamsMeta extends BaseTransformMeta implements TransformMetaInterface {
  private static Class<?> PKG = PrioritizeStreamsMeta.class; // for i18n purposes, needed by Translator!!

  /**
   * by which transforms to display?
   */
  private String[] transformName;

  public PrioritizeStreamsMeta() {
    super(); // allocate BaseTransformMeta
  }

  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode, metaStore );
  }

  public Object clone() {
    PrioritizeStreamsMeta retval = (PrioritizeStreamsMeta) super.clone();

    int nrFields = transformName.length;

    retval.allocate( nrFields );
    System.arraycopy( transformName, 0, retval.transformName, 0, nrFields );
    return retval;
  }

  public void allocate( int nrFields ) {
    transformName = new String[ nrFields ];
  }

  /**
   * @return Returns the transformName.
   */
  public String[] getTransformName() {
    return transformName;
  }

  /**
   * @param transformName The transformName to set.
   */
  public void setTransformName( String[] transformName ) {
    this.transformName = transformName;
  }

  public void getFields( IRowMeta rowMeta, String origin, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {
    // Default: nothing changes to rowMeta
  }

  private void readData( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    try {
      Node transforms = XMLHandler.getSubNode( transformNode, "transforms" );
      int nrTransforms = XMLHandler.countNodes( transforms, "transform" );

      allocate( nrTransforms );

      for ( int i = 0; i < nrTransforms; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( transforms, "transform", i );
        transformName[ i ] = XMLHandler.getTagValue( fnode, "name" );
      }
    } catch ( Exception e ) {
      throw new HopXMLException( "Unable to load transform info from XML", e );
    }
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "    <transforms>" + Const.CR );
    for ( int i = 0; i < transformName.length; i++ ) {
      retval.append( "      <transform>" + Const.CR );
      retval.append( "        " + XMLHandler.addTagValue( "name", transformName[ i ] ) );
      retval.append( "        </transform>" + Const.CR );
    }
    retval.append( "      </transforms>" + Const.CR );

    return retval.toString();
  }

  public void setDefault() {
    int nrTransforms = 0;

    allocate( nrTransforms );

    for ( int i = 0; i < nrTransforms; i++ ) {
      transformName[ i ] = "transform" + i;
    }
  }

  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    CheckResult cr;

    if ( prev == null || prev.size() == 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(
          PKG, "PrioritizeStreamsMeta.CheckResult.NotReceivingFields" ), transformMeta );
      remarks.add( cr );
    } else {
      if ( transformName.length > 0 ) {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
            PKG, "PrioritizeStreamsMeta.CheckResult.AllTransformsFound" ), transformMeta );
        remarks.add( cr );
      } else {
        cr =
          new CheckResult( CheckResult.TYPE_RESULT_WARNING, BaseMessages.getString(
            PKG, "PrioritizeStreamsMeta.CheckResult.NoTransformsEntered" ), transformMeta );
        remarks.add( cr );
      }

    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "PrioritizeStreamsMeta.CheckResult.TransformRecevingData2" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResult.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "PrioritizeStreamsMeta.CheckResult.NoInputReceivedFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    }
  }

  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr, PipelineMeta tr,
                                Pipeline pipeline ) {
    return new PrioritizeStreams( transformMeta, this, data, cnr, tr, pipeline );
  }

  public ITransformData getTransformData() {
    return new PrioritizeStreamsData();
  }

}
