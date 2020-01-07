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
