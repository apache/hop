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

package org.apache.hop.pipeline.steps.blockingstep;

import org.apache.hop.core.CheckResult;
import org.apache.hop.core.CheckResultInterface;
import org.apache.hop.core.Const;
import org.apache.hop.core.annotations.Step;
import org.apache.hop.core.exception.HopStepException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.row.RowMetaInterface;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.step.BaseStepMeta;
import org.apache.hop.pipeline.step.StepDataInterface;
import org.apache.hop.pipeline.step.StepInterface;
import org.apache.hop.pipeline.step.StepMeta;
import org.apache.hop.pipeline.step.StepMetaInterface;
import org.w3c.dom.Node;

import java.io.File;
import java.util.List;

@Step( id = "BlockingStep", i18nPackageName = "org.apache.hop.pipeline.steps.blockingstep", name = "BlockingStep.Name",
  description = "BlockingStep.Description",
  categoryDescription = "i18n:org.apache.hop.pipeline.step:BaseStep.Category.Flow" )
public class BlockingStepMeta extends BaseStepMeta implements StepMetaInterface {

  private static Class<?> PKG = BlockingStepMeta.class; // for i18n purposes, needed by Translator!!

  /**
   * Directory to store the temp files
   */
  private String directory;

  /**
   * Temp files prefix...
   */
  private String prefix;

  /**
   * The cache size: number of rows to keep in memory
   */
  private int cacheSize;

  /**
   * Compress files: if set to true, temporary files are compressed, thus reducing I/O at the cost of slightly higher
   * CPU usage
   */
  private boolean compressFiles;

  /**
   * Pass all rows, or only the last one. Only the last row was the original behaviour.
   */
  private boolean passAllRows;

  /**
   * Cache size: how many rows do we keep in memory
   */
  public static final int CACHE_SIZE = 5000;

  @Override
  public void check( List<CheckResultInterface> remarks, PipelineMeta pipelineMeta, StepMeta stepMeta, RowMetaInterface prev,
                     String[] input, String[] output, RowMetaInterface info, VariableSpace space, IMetaStore metaStore ) {
    CheckResult cr;

    if ( prev != null && prev.size() > 0 ) {
      // Check the sort directory
      String realDirectory = pipelineMeta.environmentSubstitute( directory );

      File f = new File( realDirectory );
      if ( f.exists() ) {
        if ( f.isDirectory() ) {
          cr =
            new CheckResult( CheckResultInterface.TYPE_RESULT_OK,
              BaseMessages.getString( PKG, "BlockingStepMeta.CheckResult.DirectoryExists", realDirectory ),
              stepMeta );
          remarks.add( cr );
        } else {
          cr =
            new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR,
              BaseMessages.getString( PKG, "BlockingStepMeta.CheckResult.ExistsButNoDirectory", realDirectory ),
              stepMeta );
          remarks.add( cr );
        }
      } else {
        cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR,
            BaseMessages.getString( PKG, "BlockingStepMeta.CheckResult.DirectoryNotExists", realDirectory ),
            stepMeta );
        remarks.add( cr );
      }
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR,
          BaseMessages.getString( PKG, "BlockingStepMeta.CheckResult.NoFields" ), stepMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_OK,
          BaseMessages.getString( PKG, "BlockingStepMeta.CheckResult.StepExpectingRowsFromOtherSteps" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
        new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR,
          BaseMessages.getString( PKG, "BlockingStepMeta.CheckResult.NoInputReceivedError" ), stepMeta );
      remarks.add( cr );
    }
  }

  @Override
  public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, IMetaStore metaStore )
    throws HopStepException {
    // Default: no values are added to the row in the step
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, PipelineMeta pipelineMeta,
                                Pipeline pipeline ) {
    return new BlockingStep( stepMeta, stepDataInterface, copyNr, pipelineMeta, pipeline );
  }

  @Override
  public StepDataInterface getStepData() {
    return new BlockingStepData();
  }

  @Override
  public void loadXML( Node stepnode, IMetaStore metaStore ) throws HopXMLException {
    readData( stepnode );
  }

  private void readData( Node stepnode ) {
    passAllRows = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "pass_all_rows" ) );
    directory = XMLHandler.getTagValue( stepnode, "directory" );
    prefix = XMLHandler.getTagValue( stepnode, "prefix" );
    cacheSize = Const.toInt( XMLHandler.getTagValue( stepnode, "cache_size" ), CACHE_SIZE );
    compressFiles = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "compress" ) );
  }

  public void setDefault() {
    passAllRows = false;
    directory = "%%java.io.tmpdir%%";
    prefix = "block";
    cacheSize = CACHE_SIZE;
    compressFiles = true;
  }

  public String getXML() {
    StringBuilder retval = new StringBuilder( 300 );

    retval.append( "      " ).append( XMLHandler.addTagValue( "pass_all_rows", passAllRows ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "directory", directory ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "prefix", prefix ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "cache_size", cacheSize ) );
    retval.append( "      " ).append( XMLHandler.addTagValue( "compress", compressFiles ) );

    return retval.toString();
  }

  /**
   * @return Returns the cacheSize.
   */
  public int getCacheSize() {
    return cacheSize;
  }

  /**
   * @param cacheSize The cacheSize to set.
   */
  public void setCacheSize( int cacheSize ) {
    this.cacheSize = cacheSize;
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
   * @return Returns whether temporary files should be compressed
   */
  public boolean getCompress() {
    return compressFiles;
  }

  /**
   * @param compressFiles Whether to compress temporary files created during sorting
   */
  public void setCompress( boolean compressFiles ) {
    this.compressFiles = compressFiles;
  }

  /**
   * @return true when all rows are passed and false when only the last one is passed.
   */
  public boolean isPassAllRows() {
    return passAllRows;
  }

  /**
   * @param passAllRows set to true if all rows should be passed and false if only the last one should be passed
   */
  public void setPassAllRows( boolean passAllRows ) {
    this.passAllRows = passAllRows;
  }

  /**
   * @return The directory to store the temporary files in.
   */
  public String getDirectory() {
    return directory;
  }

  /**
   * Set the directory to store the temp files in.
   */
  public void setDirectory( String directory ) {
    this.directory = directory;
  }

  @Override
  public PipelineType[] getSupportedPipelineTypes() {
    return new PipelineType[] {
      PipelineType.Normal, };
  }
}
