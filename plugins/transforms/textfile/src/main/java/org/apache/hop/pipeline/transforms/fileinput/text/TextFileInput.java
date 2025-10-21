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
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.playlist.FilePlayListAll;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.file.BaseFileField;
import org.apache.hop.pipeline.transforms.file.BaseFileInputTransform;
import org.apache.hop.pipeline.transforms.file.IBaseFileInputReader;
import org.apache.hop.staticschema.metadata.SchemaDefinition;
import org.apache.hop.staticschema.metadata.SchemaFieldDefinition;
import org.apache.hop.staticschema.util.SchemaDefinitionUtil;

/**
 * Read all sorts of text files, convert them to rows and writes these to one or more output
 * streams.
 */
public class TextFileInput extends BaseFileInputTransform<TextFileInputMeta, TextFileInputData> {

  private static final Class<?> PKG = TextFileInputMeta.class;

  public TextFileInput(
      TransformMeta transformMeta,
      TextFileInputMeta meta,
      TextFileInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  protected IBaseFileInputReader createReader(
      TextFileInputMeta meta, TextFileInputData data, FileObject file) throws Exception {
    return new TextFileInputReader(this, meta, data, file, getLogChannel());
  }

  @Override
  public boolean init() {
    if (!super.init()) {
      return false;
    }
    data.filePlayList = FilePlayListAll.INSTANCE;

    data.filterProcessor = new TextFileFilterProcessor(meta.getFilter(), this);

    // calculate the file format type in advance so we can use a switch
    data.fileFormatType = meta.getFileFormatTypeNr();

    // calculate the file type in advance CSV or Fixed?
    data.fileType = meta.getFileTypeNr();

    // Handle the possibility of a variable substitution
    data.separator = resolve(meta.content.separator);
    data.enclosure = resolve(meta.content.enclosure);
    data.escapeCharacter = resolve(meta.content.escapeCharacter);
    // CSV without separator defined
    if (meta.content.fileType.equalsIgnoreCase("CSV") && (Utils.isEmpty(meta.content.separator))) {
      logError(BaseMessages.getString(PKG, "TextFileInput.Exception.NoSeparator"));
      return false;
    }

    // If use shema and ignore fields set the field definitions in the transform
    if (meta.ignoreFields) {
      try {
        SchemaDefinition loadedSchemaDefinition =
            (new SchemaDefinitionUtil())
                .loadSchemaDefinition(metadataProvider, meta.getSchemaDefinition());
        if (loadedSchemaDefinition != null) {
          IRowMeta r = loadedSchemaDefinition.getRowMeta();
          if (r != null) {
            meta.inputFields = new BaseFileField[r.size()];
            for (int i = 0; i < r.size(); i++) {
              final SchemaFieldDefinition schemaFieldDefinition =
                  loadedSchemaDefinition.getFieldDefinitions().get(i);
              BaseFileField f = new BaseFileField();
              f.setName(schemaFieldDefinition.getName());
              f.setType(r.getValueMeta(i).getType());
              f.setFormat(r.getValueMeta(i).getFormatMask());
              f.setLength(r.getValueMeta(i).getLength());
              f.setPrecision(r.getValueMeta(i).getPrecision());
              f.setCurrencySymbol(r.getValueMeta(i).getCurrencySymbol());
              f.setDecimalSymbol(r.getValueMeta(i).getDecimalSymbol());
              f.setGroupSymbol(r.getValueMeta(i).getGroupingSymbol());
              f.setIfNullValue(schemaFieldDefinition.getIfNullValue());
              f.setTrimType(r.getValueMeta(i).getTrimType());
              f.setRepeated(false);
              meta.inputFields[i] = f;
            }
          }
        }
      } catch (HopTransformException | HopPluginException e) {
        // ignore any errors here.
      }
    }

    return true;
  }
}
