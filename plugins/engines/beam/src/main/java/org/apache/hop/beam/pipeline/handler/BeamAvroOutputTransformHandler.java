/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.beam.pipeline.handler;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.avro.transforms.avrooutput.AvroOutput;
import org.apache.hop.avro.transforms.avrooutput.AvroOutputField;
import org.apache.hop.avro.transforms.avrooutput.AvroOutputMeta;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.fn.AvroFieldMapping;
import org.apache.hop.beam.core.fn.HopRowToAvroFn;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.pipeline.IBeamPipelineTransformHandler;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Issue #2358: binary-file Avro output on Beam.
 *
 * <p>One file, written with {@code AvroIO.writeGenericRecords} and {@code withoutSharding()}. Date
 * and time are resolved on the machine that submits the pipeline. Binary-field and JSON-field
 * output keep the generic handler, because those modes add a column to the row instead of writing a
 * file.
 */
public class BeamAvroOutputTransformHandler extends BeamBaseTransformHandler
    implements IBeamPipelineTransformHandler {

  @Override
  public boolean isInput() {
    return false;
  }

  @Override
  public boolean isOutput() {
    return false;
  }

  @Override
  public void handleTransform(
      ILogChannel log,
      IVariables variables,
      String runConfigurationName,
      IBeamPipelineEngineRunConfiguration runConfiguration,
      String dataSamplersJson,
      IHopMetadataProvider metadataProvider,
      PipelineMeta pipelineMeta,
      TransformMeta transformMeta,
      Map<String, PCollection<HopRow>> transformCollectionMap,
      Pipeline pipeline,
      IRowMeta rowMeta,
      List<TransformMeta> previousTransforms,
      PCollection<HopRow> input,
      String parentLogChannelId)
      throws HopException {

    if (input == null || previousTransforms == null || previousTransforms.isEmpty()) {
      throw new HopException(
          "Avro output '" + transformMeta.getName() + "' needs one incoming transform");
    }

    AvroOutputMeta meta = new AvroOutputMeta();
    loadTransformMetadata(meta, transformMeta, metadataProvider, pipelineMeta);

    if (!AvroOutputMeta.OUTPUT_TYPES[AvroOutputMeta.OUTPUT_TYPE_BINARY_FILE].equals(
        meta.getOutputType())) {
      new BeamGenericTransformHandler()
          .handleTransform(
              log,
              variables,
              runConfigurationName,
              runConfiguration,
              dataSamplersJson,
              metadataProvider,
              pipelineMeta,
              transformMeta,
              transformCollectionMap,
              pipeline,
              rowMeta,
              previousTransforms,
              input,
              parentLogChannelId);
      return;
    }

    if (meta.isTransformNrInFilename() || meta.isPartNrInFilename()) {
      throw new HopException(
          "Avro output '"
              + transformMeta.getName()
              + "' cannot put a transform copy or a partition number in the filename on Beam."
              + " The Beam write is one file for the whole collection.");
    }
    if (rowMeta == null) {
      throw new HopException(
          "Avro output '" + transformMeta.getName() + "' has no incoming row layout");
    }

    String filename = meta.buildFilename(meta.getFileName(), variables, 0, null, false, null, 0);
    if (Utils.isEmpty(filename)) {
      throw new HopException("Avro output '" + transformMeta.getName() + "' has no filename");
    }

    List<AvroOutputField> fields = copyFields(meta);
    if (fields.isEmpty()) {
      throw new HopException("Avro output '" + transformMeta.getName() + "' has no output fields");
    }
    List<AvroFieldMapping> mappings = mappings(transformMeta.getName(), fields, rowMeta);

    Schema schema;
    if (meta.isCreateSchemaFile()) {
      schema =
          AvroOutput.createAvroSchema(
              new ArrayList<>(fields),
              "",
              meta.getDoc(),
              meta.getRecordName(),
              meta.getNamespace());
      if (meta.isWriteSchemaFile()) {
        writeSchemaFile(meta, variables, schema);
      }
    } else {
      schema = readSchemaFile(meta, variables);
    }

    createParentIfRequested(meta, filename);

    PCollection<GenericRecord> records =
        input
            .apply(
                transformMeta.getName() + " To Avro",
                ParDo.of(
                    new HopRowToAvroFn(
                        transformMeta.getName(),
                        JsonRowMeta.toJson(rowMeta),
                        schema.toString(),
                        mappings)))
            .setCoder(AvroCoder.of(schema));

    AvroIO.Write<GenericRecord> write =
        AvroIO.writeGenericRecords(schema).to(filename).withoutSharding();
    String compression = meta.getCompressionType();
    if (!Utils.isEmpty(compression) && !"none".equalsIgnoreCase(compression)) {
      write = write.withCodec(CodecFactory.fromString(compression));
    }
    records.apply(transformMeta.getName() + " Write", write);

    // Binary-file output passes the incoming row on, the same way the local transform does.
    transformCollectionMap.put(transformMeta.getName(), input);
    log.logBasic(
        "Handled transform (AVRO OUTPUT) : " + transformMeta.getName() + ", file " + filename);
  }

  private static List<AvroOutputField> copyFields(AvroOutputMeta meta) {
    List<AvroOutputField> fields = new ArrayList<>();
    if (meta.getOutputFields() == null) {
      return fields;
    }
    for (AvroOutputField field : meta.getOutputFields()) {
      if (field != null) {
        fields.add(new AvroOutputField(field));
      }
    }
    return fields;
  }

  private static List<AvroFieldMapping> mappings(
      String transformName, List<AvroOutputField> fields, IRowMeta rowMeta) throws HopException {
    List<AvroFieldMapping> mappings = new ArrayList<>();
    for (AvroOutputField field : fields) {
      String avroName = field.getAvroName();
      if (avroName.startsWith("$.")) {
        avroName = avroName.substring(2);
      }
      if (avroName.contains(".")) {
        throw new HopException(
            "Avro output '"
                + transformName
                + "' cannot write nested path '"
                + avroName
                + "' on Beam");
      }
      if (field.getAvroType() == AvroOutputField.AVRO_TYPE_ENUM) {
        throw new HopException(
            "Avro output '"
                + transformName
                + "' cannot write enum field '"
                + avroName
                + "' on Beam");
      }
      if (rowMeta.indexOfValue(field.getName()) < 0) {
        throw new HopException(
            "Field [" + field.getName() + "] couldn't be found in the input stream!");
      }
      mappings.add(new AvroFieldMapping(field.getName(), avroName, field.getAvroType()));
    }
    return mappings;
  }

  private static void writeSchemaFile(AvroOutputMeta meta, IVariables variables, Schema schema)
      throws HopException {
    if (Utils.isEmpty(meta.getSchemaFileName())) {
      throw new HopException(
          "Avro output is set to write a schema file, but no schema filename is set");
    }
    String schemaFilename =
        meta.buildFilename(meta.getSchemaFileName(), variables, 0, null, false, null, 0);
    createParentIfRequested(meta, schemaFilename);
    try (OutputStream out = HopVfs.getOutputStream(schemaFilename, false)) {
      out.write(schema.toString(true).getBytes(StandardCharsets.UTF_8));
    } catch (Exception e) {
      throw new HopException("Unable to write Avro schema file " + schemaFilename, e);
    }
  }

  private static Schema readSchemaFile(AvroOutputMeta meta, IVariables variables)
      throws HopException {
    if (Utils.isEmpty(meta.getSchemaFileName())) {
      throw new HopException("Avro output has no schema file and is not creating one");
    }
    String schemaFilename = variables.resolve(meta.getSchemaFileName());
    try (InputStream in = HopVfs.getInputStream(schemaFilename, variables)) {
      return new Schema.Parser().parse(in);
    } catch (Exception e) {
      throw new HopException("Unable to read Avro schema file " + schemaFilename, e);
    }
  }

  private static void createParentIfRequested(AvroOutputMeta meta, String filename)
      throws HopException {
    FileObject parent = null;
    try {
      parent = HopVfs.getFileObject(filename).getParent();
      if (parent != null && !parent.exists()) {
        if (meta.isCreateParentFolder()) {
          parent.createFolder();
        } else {
          throw new HopException("Parent folder does not exist for " + filename);
        }
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Unable to prepare the parent folder for " + filename, e);
    } finally {
      if (parent != null) {
        try {
          parent.close();
        } catch (Exception ignored) {
          // closing the parent is best-effort
        }
      }
    }
  }
}
