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

package org.apache.hop.trans.steps.textfileoutputlegacy;

import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopXMLException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.core.xml.XMLHandler;
import org.apache.hop.repository.ObjectId;
import org.apache.hop.repository.Repository;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.StepDataInterface;
import org.apache.hop.trans.step.StepInterface;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.trans.steps.textfileoutput.TextFileOutputMeta;
import org.apache.hop.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.List;

/**
 * This is deprecated version with capability run as command.
 * @deprecated use {@link org.apache.hop.trans.steps.textfileoutput.TextFileOutputMeta} instead.
 */
@Deprecated
public class TextFileOutputLegacyMeta extends TextFileOutputMeta {

   /** Whether to treat this as a command to be executed and piped into */
  @Injection( name = "RUN_AS_COMMAND" )
  private boolean fileAsCommand;

  public TextFileOutputLegacyMeta() {
    super(); // allocate BaseStepMeta
  }

  /**
   * @return FileAsCommand
   */
  public boolean isFileAsCommand() {
    return fileAsCommand;
  }

  /**
   * @param fileAsCommand
   *          The fileAsCommand to set
   */
  public void setFileAsCommand( boolean fileAsCommand ) {
    this.fileAsCommand = fileAsCommand;
  }

  protected void readData( Node stepnode, IMetaStore metastore ) throws HopXMLException {
    super.readData( stepnode, metastore );
    try {
      fileAsCommand = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "file", "is_command" ) );
    } catch ( Exception e ) {
      throw new HopXMLException( "Unable to load step info from XML", e );
    }
  }

  @Override
  public void setDefault() {
    super.setDefault();
    fileAsCommand = false;
  }

  @Override
  protected void saveFileOptions( StringBuilder retval ) {
    super.saveFileOptions( retval );
    retval.append( "      " ).append( XMLHandler.addTagValue( "is_command", fileAsCommand ) );
  }

  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws HopException {
    super.readRep( rep, metaStore, id_step, databases );
    try {
      fileAsCommand = rep.getStepAttributeBoolean( id_step, "file_is_command" );
    } catch ( Exception e ) {
      throw new HopException( "Unexpected error reading step information from the repository", e );
    }
  }

  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
    throws HopException {
    super.saveRep( rep, metaStore, id_transformation, id_step );
    try {
      rep.saveStepAttribute( id_transformation, id_step, "file_is_command", fileAsCommand );
    } catch ( Exception e ) {
      throw new HopException( "Unable to save step information to the repository for id_step=" + id_step, e );
    }
  }

  @Override
  public String buildFilename( String filename, String extension, VariableSpace space, int stepnr, String partnr,
                               int splitnr, boolean ziparchive, TextFileOutputMeta meta ) {
    if ( ( (TextFileOutputLegacyMeta) meta ).isFileAsCommand() ) {
      return space.environmentSubstitute( filename );
    } else {
      return super.buildFilename( filename, extension, space, stepnr, partnr, splitnr, ziparchive, meta );
    }
  }


  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
                                Trans trans ) {
    return new TextFileOutputLegacy( stepMeta, stepDataInterface, cnr, transMeta, trans );
  }

}
