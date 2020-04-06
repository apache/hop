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

package org.apache.hop.pipeline.transforms.joinrows;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Condition;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
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
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.w3c.dom.Node;

import java.io.File;
import java.util.List;

/*
 * Created on 02-jun-2003
 *
 */
@InjectionSupported( localizationPrefix = "JoinRows.Injection." )
public class JoinRowsMeta extends BaseTransformMeta implements ITransform {
  private static Class<?> PKG = JoinRowsMeta.class; // for i18n purposes, needed by Translator!!

  @Injection( name = "TEMP_DIR" )
  private String directory;
  @Injection( name = "TEMP_FILE_PREFIX" )
  private String prefix;
  @Injection( name = "MAX_CACHE_SIZE" )
  private int cacheSize;

  /**
   * Which transform is providing the lookup data?
   */
  private TransformMeta mainTransform;

  /**
   * Which transform is providing the lookup data?
   */
  @Injection( name = "MAIN_TRANSFORM" )
  private String mainTransformName;

  /**
   * Optional condition to limit the join (where clause)
   */
  private Condition condition;

  /**
   * @return Returns the lookupFromTransform.
   */
  public TransformMeta getMainTransform() {
    return mainTransform;
  }

  /**
   * @param lookupFromTransform The lookupFromTransform to set.
   */
  public void setMainTransform( TransformMeta lookupFromTransform ) {
    this.mainTransform = lookupFromTransform;
  }

  /**
   * @return Returns the lookupFromTransformName.
   */
  public String getMainTransformName() {
    return mainTransformName;
  }

  /**
   * @param lookupFromTransformName The lookupFromTransformName to set.
   */
  public void setMainTransformName( String lookupFromTransformName ) {
    this.mainTransformName = lookupFromTransformName;
  }

  /**
   * @param cacheSize The cacheSize to set.
   */
  public void setCacheSize( int cacheSize ) {
    this.cacheSize = cacheSize;
  }

  /**
   * @return Returns the cacheSize.
   */
  public int getCacheSize() {
    return cacheSize;
  }

  /**
   * @return Returns the directory.
   */
  public String getDirectory() {
    return directory;
  }

  /**
   * @param directory The directory to set.
   */
  public void setDirectory( String directory ) {
    this.directory = directory;
  }

  /**
   * @return Returns the prefix.
   */
  public String getPrefix() {
    return prefix;
  }

  /**
   * @param prefix The prefix to set.
   */
  public void setPrefix( String prefix ) {
    this.prefix = prefix;
  }

  /**
   * @return Returns the condition.
   */
  public Condition getCondition() {
    return condition;
  }

  /**
   * @param condition The condition to set.
   */
  public void setCondition( Condition condition ) {
    this.condition = condition;
  }

  @Injection( name = "CONDITION" )
  public void setCondition( String conditionXML ) throws Exception {
    condition = new Condition( conditionXML );
  }

  public JoinRowsMeta() {
    super(); // allocate BaseTransformMeta
    condition = new Condition();
  }

  @Override
  public void loadXML( Node transformNode, IMetaStore metaStore ) throws HopXMLException {
    readData( transformNode );
  }

  @Override
  public Object clone() {
    JoinRowsMeta retval = (JoinRowsMeta) super.clone();

    return retval;
  }

  private void readData( Node transformNode ) throws HopXMLException {
    try {
      directory = XMLHandler.getTagValue( transformNode, "directory" );
      prefix = XMLHandler.getTagValue( transformNode, "prefix" );
      cacheSize = Const.toInt( XMLHandler.getTagValue( transformNode, "cache_size" ), -1 );

      mainTransformName = XMLHandler.getTagValue( transformNode, "main" );

      Node compare = XMLHandler.getSubNode( transformNode, "compare" );
      Node condnode = XMLHandler.getSubNode( compare, "condition" );

      // The new situation...
      if ( condnode != null ) {
        condition = new Condition( condnode );
      } else {
        condition = new Condition();
      }

    } catch ( Exception e ) {
      throw new HopXMLException( BaseMessages.getString(
        PKG, "JoinRowsMeta.Exception.UnableToReadTransformMetaFromXML" ), e );
    }
  }

  @Override
  public void setDefault() {
    directory = "%%java.io.tmpdir%%";
    prefix = "out";
    cacheSize = 500;

    mainTransformName = null;
  }

  @Override
  public String getXML() throws HopException {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append( "      " ).append( XMLHandler.addTagValue( "directory", directory ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "prefix", prefix ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "cache_size", cacheSize ) );

    if ( mainTransformName == null ) {
      mainTransformName = getLookupTransformName();
    }
    retval.append( "      " ).append( XMLHandler.addTagValue( "main", mainTransformName ) );

    retval.append( "    <compare>" ).append( Const.CR );

    if ( condition != null ) {
      retval.append( condition.getXML() );
    }

    retval.append( "    </compare>" ).append( Const.CR );

    return retval.toString();
  }

  @Override
  public void getFields( IRowMeta rowMeta, String origin, IRowMeta[] info, TransformMeta nextTransform,
                         iVariables variables, IMetaStore metaStore ) throws HopTransformException {
    if ( variables instanceof PipelineMeta ) {
      PipelineMeta pipelineMeta = (PipelineMeta) variables;
      TransformMeta[] transforms = pipelineMeta.getPrevTransforms( pipelineMeta.findTransform( origin ) );
      TransformMeta mainTransform = pipelineMeta.findTransform( getMainTransformName() );
      rowMeta.clear();
      if ( mainTransform != null ) {
        rowMeta.addRowMeta( pipelineMeta.getTransformFields( mainTransform ) );
      }
      for ( TransformMeta transform : transforms ) {
        if ( mainTransform == null || !transform.equals( mainTransform ) ) {
          rowMeta.addRowMeta( pipelineMeta.getTransformFields( transform ) );
        }
      }
    }
  }

  @Override
  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, TransformMeta transformMeta,
                     IRowMeta prev, String[] input, String[] output, IRowMeta info, iVariables variables,
                     IMetaStore metaStore ) {
    CheckResult cr;

    if ( prev != null && prev.size() > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "JoinRowsMeta.CheckResult.TransformReceivingDatas", prev.size() + "" ), transformMeta );
      remarks.add( cr );

      // Check the sort directory
      String realDirectory = pipelineMeta.environmentSubstitute( directory );
      File f = new File( realDirectory );
      if ( f.exists() ) {
        if ( f.isDirectory() ) {
          cr =
            new CheckResult(
              CheckResultInterface.TYPE_RESULT_OK, "["
              + realDirectory + BaseMessages.getString( PKG, "JoinRowsMeta.CheckResult.DirectoryExists" ),
              transformMeta );
          remarks.add( cr );
        } else {
          cr =
            new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, "["
              + realDirectory
              + BaseMessages.getString( PKG, "JoinRowsMeta.CheckResult.DirectoryExistsButNotValid" ), transformMeta );
          remarks.add( cr );
        }
      } else {
        cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
            PKG, "JoinRowsMeta.CheckResult.DirectoryDoesNotExist", realDirectory ), transformMeta );
        remarks.add( cr );
      }
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "JoinRowsMeta.CheckResult.CouldNotFindFieldsFromPreviousTransforms" ), transformMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this transform!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(
          PKG, "JoinRowsMeta.CheckResult.TransformReceivingInfoFromOtherTransforms" ), transformMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(
          PKG, "JoinRowsMeta.CheckResult.NoInputReceived" ), transformMeta );
      remarks.add( cr );
    }
  }

  public String getLookupTransformName() {
    if ( mainTransform != null && mainTransform.getName() != null && mainTransform.getName().length() > 0 ) {
      return mainTransform.getName();
    }
    return null;
  }

  /**
   * @param transforms optionally search the info transform in a list of transforms
   */
  @Override
  public void searchInfoAndTargetTransforms( List<TransformMeta> transforms ) {
    mainTransform = TransformMeta.findTransform( transforms, mainTransformName );
  }

  @Override
  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr,
                                PipelineMeta pipelineMeta, Pipeline pipeline ) {
    return new JoinRows( transformMeta, this, data, cnr, pipelineMeta, pipeline );
  }

  @Override
  public ITransformData getTransformData() {
    return new JoinRowsData();
  }

  @Override
  public boolean excludeFromRowLayoutVerification() {
    return true;
  }

  @Override
  public boolean cleanAfterHopToRemove( TransformMeta fromTransform ) {
    boolean hasChanged = false;

    // If the hop we're removing comes from a Transform that is being used as the main transform for the Join, we have to clear
    // that reference
    if ( null != fromTransform && fromTransform.equals( getMainTransform() ) ) {
      setMainTransform( null );
      setMainTransformName( null );
      hasChanged = true;
    }

    return hasChanged;
  }
}
