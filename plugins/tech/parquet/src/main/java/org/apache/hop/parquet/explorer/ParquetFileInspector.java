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

package org.apache.hop.parquet.explorer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.parquet.transforms.input.ParquetField;
import org.apache.hop.parquet.transforms.input.ParquetInputMeta;
import org.apache.hop.parquet.transforms.input.ParquetReadSupport;
import org.apache.hop.parquet.transforms.input.ParquetReaderBuilder;
import org.apache.hop.parquet.transforms.input.ParquetStream;
import org.apache.hop.parquet.transforms.output.ParquetVersion;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/** Reads the footer, the first page header and a bounded row preview of one Parquet file. */
public final class ParquetFileInspector {

  /** How many rows the explorer preview reads. */
  public static final int PREVIEW_ROW_LIMIT = 1000;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private ParquetFileInspector() {}

  /**
   * Reads {@code filename} through Hop VFS. The file itself is not loaded: the footer, one page
   * header and at most {@link #PREVIEW_ROW_LIMIT} rows.
   */
  public static ParquetFileInspection inspect(String filename, IVariables variables)
      throws HopException {
    try {
      FileObject fileObject = HopVfs.getFileObject(filename, variables);
      String fileName = fileObject.getName().getBaseName();
      FileObject parent = fileObject.getParent();
      String folder = parent == null ? "" : HopVfs.getFilename(parent);
      long sizeBytes = fileObject.getContent().getSize();

      try (ParquetStream parquetStream = new ParquetStream(fileObject, filename)) {
        ParquetMetadata footer;
        try (ParquetFileReader reader = ParquetFileReader.open(parquetStream)) {
          footer = reader.getFooter();
        }
        FileMetaData fileMetaData = footer.getFileMetaData();
        MessageType schema = fileMetaData.getSchema();
        PageFacts pages = readPageFacts(parquetStream, footer);
        Preview preview = readPreview(parquetStream, schema);
        return new ParquetFileInspection(
            fileName,
            folder,
            sizeBytes,
            compression(footer),
            pages.version,
            rowGroupSize(footer),
            pages.dataPageSize,
            pages.dictionaryPageSize,
            rowCount(footer),
            footer.getBlocks().size(),
            fileMetaData.getCreatedBy() == null ? "" : fileMetaData.getCreatedBy(),
            schemaJson(schema),
            leafColumns(schema),
            preview.rowMeta,
            preview.rows,
            preview.error);
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Unable to read Parquet file '" + filename + "'", e);
    }
  }

  /** Pretty-printed JSON of the schema tree. Groups stay nested; this is not the file footer. */
  static String schemaJson(MessageType schema) throws IOException {
    ObjectNode root = MAPPER.createObjectNode();
    root.put("name", schema.getName());
    ArrayNode fields = root.putArray("fields");
    for (Type field : schema.getFields()) {
      fields.add(typeNode(field));
    }
    return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(root);
  }

  /** Leaf columns in file order. A nested field is named with a dotted path. */
  static List<ParquetColumnView> leafColumns(MessageType schema) throws HopException {
    List<ParquetColumnView> columns = new ArrayList<>();
    for (Type field : schema.getFields()) {
      collectLeaves(field, "", columns);
    }
    return columns;
  }

  private static void collectLeaves(Type type, String prefix, List<ParquetColumnView> columns)
      throws HopException {
    String name = prefix.isEmpty() ? type.getName() : prefix + "." + type.getName();
    if (type.isPrimitive()) {
      PrimitiveType primitive = type.asPrimitiveType();
      IValueMeta valueMeta = ParquetInputMeta.hopValueMeta(name, primitive);
      columns.add(
          new ParquetColumnView(
              name,
              parquetType(primitive),
              valueMeta.getTypeDesc(),
              valueMeta.getLength(),
              valueMeta.getPrecision()));
      return;
    }
    for (Type child : type.asGroupType().getFields()) {
      collectLeaves(child, name, columns);
    }
  }

  private static ObjectNode typeNode(Type type) {
    ObjectNode node = MAPPER.createObjectNode();
    node.put("name", type.getName());
    node.put("repetition", type.getRepetition().name());
    LogicalTypeAnnotation logicalType = type.getLogicalTypeAnnotation();
    if (type.isPrimitive()) {
      PrimitiveType primitive = type.asPrimitiveType();
      node.put("type", primitive.getPrimitiveTypeName().name());
      if (primitive.getTypeLength() > 0) {
        node.put("length", primitive.getTypeLength());
      }
    } else {
      node.put("type", "group");
      ArrayNode children = node.putArray("fields");
      for (Type child : type.asGroupType().getFields()) {
        children.add(typeNode(child));
      }
    }
    if (logicalType != null) {
      node.put("logicalType", logicalType.toString());
    }
    return node;
  }

  private static String parquetType(PrimitiveType primitive) {
    String physical = primitive.getPrimitiveTypeName().name();
    LogicalTypeAnnotation logicalType = primitive.getLogicalTypeAnnotation();
    if (logicalType == null) {
      return physical;
    }
    return physical + " (" + logicalType + ")";
  }

  private static String compression(ParquetMetadata footer) {
    TreeSet<String> codecs = new TreeSet<>();
    for (BlockMetaData block : footer.getBlocks()) {
      for (ColumnChunkMetaData column : block.getColumns()) {
        if (column.getCodec() != null) {
          codecs.add(column.getCodec().name());
        }
      }
    }
    return String.join(", ", codecs);
  }

  private static long rowCount(ParquetMetadata footer) {
    long rows = 0;
    for (BlockMetaData block : footer.getBlocks()) {
      rows += block.getRowCount();
    }
    return rows;
  }

  /** Largest row group, in uncompressed bytes. The writer limit is not stored in the file. */
  private static Long rowGroupSize(ParquetMetadata footer) {
    Long largest = null;
    for (BlockMetaData block : footer.getBlocks()) {
      long size = block.getTotalByteSize();
      if (largest == null || size > largest) {
        largest = size;
      }
    }
    return largest;
  }

  /**
   * Version and page sizes from the first column chunk only. The dictionary page and the first data
   * page headers are a few dozen bytes; the column values are not read.
   */
  private static PageFacts readPageFacts(ParquetStream parquetStream, ParquetMetadata footer)
      throws IOException {
    if (footer.getBlocks().isEmpty() || footer.getBlocks().get(0).getColumns().isEmpty()) {
      return PageFacts.empty();
    }
    ColumnChunkMetaData column = footer.getBlocks().get(0).getColumns().get(0);
    Long dictionaryPageSize = null;
    Long dataPageSize = null;
    String version = "";
    try (SeekableInputStream in = parquetStream.newStream()) {
      if (column.hasDictionaryPage() && column.getDictionaryPageOffset() > 0) {
        PageHeader header = readHeader(in, column.getDictionaryPageOffset());
        if (header.getType() == PageType.DICTIONARY_PAGE) {
          dictionaryPageSize = (long) header.getUncompressed_page_size();
        }
      }
      if (column.getFirstDataPageOffset() > 0) {
        PageHeader header = readHeader(in, column.getFirstDataPageOffset());
        dataPageSize = (long) header.getUncompressed_page_size();
        if (header.getType() == PageType.DATA_PAGE_V2) {
          version = ParquetVersion.Version2.getDescription();
        } else if (header.getType() == PageType.DATA_PAGE) {
          version = ParquetVersion.Version1.getDescription();
        }
      }
    }
    return new PageFacts(version, dataPageSize, dictionaryPageSize);
  }

  private static PageHeader readHeader(SeekableInputStream in, long offset) throws IOException {
    in.seek(offset);
    return Util.readPageHeader(in);
  }

  /**
   * Top-level primitive columns, which is what {@link ParquetReadSupport} can convert. Nested
   * values stay in the schema JSON and the leaf table.
   */
  private static Preview readPreview(ParquetStream parquetStream, MessageType schema) {
    IRowMeta rowMeta = new RowMeta();
    List<ParquetField> fields = new ArrayList<>();
    try {
      for (ColumnDescriptor column : schema.getColumns()) {
        if (column.getPath().length != 1) {
          continue;
        }
        String name = column.getPath()[0];
        IValueMeta valueMeta = ParquetInputMeta.hopValueMeta(name, column.getPrimitiveType());
        rowMeta.addValueMeta(valueMeta);
        fields.add(
            new ParquetField(
                name,
                name,
                valueMeta.getTypeDesc(),
                valueMeta.getFormatMask(),
                Integer.toString(valueMeta.getLength()),
                Integer.toString(valueMeta.getPrecision())));
      }
      if (fields.isEmpty()) {
        return new Preview(rowMeta, List.of(), null);
      }
      List<Object[]> rows = new ArrayList<>();
      ParquetReadSupport readSupport = new ParquetReadSupport(fields);
      try (ParquetReader<RowMetaAndData> reader =
          new ParquetReaderBuilder<>(readSupport, parquetStream).build()) {
        RowMetaAndData row = reader.read();
        while (row != null && rows.size() < PREVIEW_ROW_LIMIT) {
          Object[] data = row.getData();
          rows.add(data == null ? new Object[rowMeta.size()] : Arrays.copyOf(data, data.length));
          if (rows.size() >= PREVIEW_ROW_LIMIT) {
            break;
          }
          row = reader.read();
        }
      }
      return new Preview(rowMeta, rows, null);
    } catch (Exception e) {
      String message = e.getMessage() == null ? e.toString() : e.getMessage();
      return new Preview(rowMeta, List.of(), message);
    }
  }

  private record PageFacts(String version, Long dataPageSize, Long dictionaryPageSize) {
    static PageFacts empty() {
      return new PageFacts("", null, null);
    }
  }

  private record Preview(IRowMeta rowMeta, List<Object[]> rows, String error) {}
}
