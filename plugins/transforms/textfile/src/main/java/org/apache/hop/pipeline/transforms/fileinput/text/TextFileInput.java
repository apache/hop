/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.fileinput.text;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.playlist.FilePlayListAll;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.file.BaseFileInputTransform;
import org.apache.hop.pipeline.transforms.file.IBaseFileInputReader;

/**
 * Read all sorts of text files, convert them to rows and writes these to one or more output streams.
 *
 * @author Matt
 * @since 4-apr-2003
 */
public class TextFileInput
  extends BaseFileInputTransform<TextFileInputMeta, TextFileInputData>
  implements ITransform<TextFileInputMeta, TextFileInputData> {

  private static final Class<?> PKG = TextFileInputMeta.class; // For Translator

  public TextFileInput( TransformMeta transformMeta, TextFileInputMeta meta, TextFileInputData data, int copyNr, PipelineMeta pipelineMeta,
                        Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override
  protected IBaseFileInputReader createReader( TextFileInputMeta meta, TextFileInputData data, FileObject file )
    throws Exception {
    return new TextFileInputReader( this, meta, data, file, log );
  }

  @Override
  public boolean init() {
    if (!super.init()) {
      return false;
    }
    data.filePlayList = FilePlayListAll.INSTANCE;

    data.filterProcessor = new TextFileFilterProcessor( meta.getFilter(), this );

    // calculate the file format type in advance so we can use a switch
    data.fileFormatType = meta.getFileFormatTypeNr();

    // calculate the file type in advance CSV or Fixed?
    data.fileType = meta.getFileTypeNr();

    // Handle the possibility of a variable substitution
    data.separator = resolve( meta.content.separator );
    data.enclosure = resolve( meta.content.enclosure );
    data.escapeCharacter = resolve( meta.content.escapeCharacter );
    // CSV without separator defined
    if ( meta.content.fileType.equalsIgnoreCase( "CSV" ) && ( meta.content.separator == null || meta.content.separator
      .isEmpty() ) ) {
      logError( BaseMessages.getString( PKG, "TextFileInput.Exception.NoSeparator" ) );
      return false;
    }

    return true;
  }
}
