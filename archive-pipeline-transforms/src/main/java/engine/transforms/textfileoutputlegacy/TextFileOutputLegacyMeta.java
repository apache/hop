/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
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

package org.apache.hop.pipeline.transforms.textfileoutputlegacy;

import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.textfileoutput.TextFileOutputMeta;
import org.w3c.dom.Node;

/**
 * This is deprecated version with capability run as command.
 *
 * @deprecated use {@link org.apache.hop.pipeline.transforms.textfileoutput.TextFileOutputMeta} instead.
 */
@Deprecated
public class TextFileOutputLegacyMeta extends TextFileOutputMeta {

  /**
   * Whether to treat this as a command to be executed and piped into
   */
  @Injection( name = "RUN_AS_COMMAND" )
  private boolean fileAsCommand;

  public TextFileOutputLegacyMeta() {
    super(); // allocate BaseTransformMeta
  }

  /**
   * @return FileAsCommand
   */
  public boolean isFileAsCommand() {
    return fileAsCommand;
  }

  /**
   * @param fileAsCommand The fileAsCommand to set
   */
  public void setFileAsCommand( boolean fileAsCommand ) {
    this.fileAsCommand = fileAsCommand;
  }

  protected void readData( Node transformNode, IHopMetadataProvider metadataProvider ) throws HopXmlException {
    super.readData( transformNode, metadataProvider );
    try {
      fileAsCommand = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, "file", "is_command" ) );
    } catch ( Exception e ) {
      throw new HopXmlException( "Unable to load transform info from XML", e );
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
    retval.append( "      " ).append( XmlHandler.addTagValue( "is_command", fileAsCommand ) );
  }

  @Override
  public String buildFilename( String filename, String extension, IVariables variables, int transformnr, String partnr,
                               int splitnr, boolean ziparchive, TextFileOutputMeta meta ) {
    if ( ( (TextFileOutputLegacyMeta) meta ).isFileAsCommand() ) {
      return variables.environmentSubstitute( filename );
    } else {
      return super.buildFilename( filename, extension, variables, transformnr, partnr, splitnr, ziparchive, meta );
    }
  }


  @Override
  public ITransform getTransform( TransformMeta transformMeta, ITransformData data, int cnr, PipelineMeta pipelineMeta,
                                Pipeline pipeline ) {
    return new TextFileOutputLegacy( transformMeta, this, data, cnr, pipelineMeta, pipeline );
  }

}
