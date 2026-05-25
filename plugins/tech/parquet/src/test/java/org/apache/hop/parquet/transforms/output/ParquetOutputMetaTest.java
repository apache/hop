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

package org.apache.hop.parquet.transforms.output;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;

/** Unit test for {@link ParquetOutputMeta} */
class ParquetOutputMetaTest {

  @Test
  void testDefaultValues() {
    ParquetOutputMeta meta = new ParquetOutputMeta();
    assertEquals("yyyyMMdd-HHmmss", meta.getFilenameDateTimeFormat());
    assertEquals(CompressionCodecName.UNCOMPRESSED, meta.getCompressionCodec());
    assertEquals(ParquetVersion.Version2, meta.getVersion());
    assertEquals("268435456", meta.getRowGroupSize());
    assertEquals("8192", meta.getDataPageSize());
    assertEquals(
        Integer.toString(ParquetProperties.DEFAULT_DICTIONARY_PAGE_SIZE),
        meta.getDictionaryPageSize());
    assertEquals("1000000", meta.getFileSplitSize());
    assertTrue(meta.isFilenameIncludingCopyNr());
    assertTrue(meta.isFilenameIncludingSplitNr());
    assertTrue(meta.isFilenameCreatingParentFolders());
    assertFalse(meta.isFilenameIncludingDate());
    assertFalse(meta.isFilenameIncludingTime());
    assertFalse(meta.isFilenameIncludingDateTime());
    assertTrue(meta.getFields().isEmpty());
  }

  @Test
  void testCopyConstructor() {
    ParquetOutputMeta original = new ParquetOutputMeta();
    original.setFilenameBase("/tmp/output");
    original.setFilenameExtension("pq");
    original.setFilenameIncludingDate(true);
    original.setCompressionCodec(CompressionCodecName.SNAPPY);
    original.setVersion(ParquetVersion.Version1);
    original.setFields(List.of(new ParquetField("id", "id_out")));

    ParquetOutputMeta copy = new ParquetOutputMeta(original);
    assertEquals("/tmp/output", copy.getFilenameBase());
    assertEquals("pq", copy.getFilenameExtension());
    assertTrue(copy.isFilenameIncludingDate());
    assertEquals(CompressionCodecName.SNAPPY, copy.getCompressionCodec());
    assertEquals(ParquetVersion.Version1, copy.getVersion());
    assertEquals(1, copy.getFields().size());
    assertEquals("id", copy.getFields().get(0).getSourceFieldName());
  }

  @Test
  void testXmlRoundTrip() throws Exception {
    ParquetOutputMeta meta = new ParquetOutputMeta();
    meta.setFilenameBase("/data/out");
    meta.setFilenameExtension("parquet");
    meta.setFilenameIncludingDate(true);
    meta.setFilenameIncludingTime(true);
    meta.setFilenameIncludingDateTime(true);
    meta.setFilenameDateTimeFormat("yyyy-MM-dd");
    meta.setFilenameIncludingCopyNr(false);
    meta.setFilenameIncludingSplitNr(false);
    meta.setFileSplitSize("500");
    meta.setFilenameCreatingParentFolders(false);
    meta.setCompressionCodec(CompressionCodecName.GZIP);
    meta.setVersion(ParquetVersion.Version1);
    meta.setRowGroupSize("1024");
    meta.setDataPageSize("512");
    meta.setDictionaryPageSize("256");
    meta.getFields().add(new ParquetField("id", "id"));
    meta.getFields().add(new ParquetField("name", "name"));

    String xml =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + XmlMetadataUtil.serializeObjectToXml(meta)
            + XmlHandler.closeTag(TransformMeta.XML_TAG);

    ParquetOutputMeta loaded = new ParquetOutputMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(xml, TransformMeta.XML_TAG),
        ParquetOutputMeta.class,
        loaded,
        new MemoryMetadataProvider());

    validate(meta, loaded);
  }

  private static void validate(ParquetOutputMeta expected, ParquetOutputMeta actual) {
    assertEquals(expected.getFilenameBase(), actual.getFilenameBase());
    assertEquals(expected.getFilenameExtension(), actual.getFilenameExtension());
    assertEquals(expected.isFilenameIncludingDate(), actual.isFilenameIncludingDate());
    assertEquals(expected.isFilenameIncludingTime(), actual.isFilenameIncludingTime());
    assertEquals(expected.isFilenameIncludingDateTime(), actual.isFilenameIncludingDateTime());
    assertEquals(expected.getFilenameDateTimeFormat(), actual.getFilenameDateTimeFormat());
    assertEquals(expected.isFilenameIncludingCopyNr(), actual.isFilenameIncludingCopyNr());
    assertEquals(expected.isFilenameIncludingSplitNr(), actual.isFilenameIncludingSplitNr());
    assertEquals(expected.getFileSplitSize(), actual.getFileSplitSize());
    assertEquals(
        expected.isFilenameCreatingParentFolders(), actual.isFilenameCreatingParentFolders());
    assertEquals(expected.getCompressionCodec(), actual.getCompressionCodec());
    assertEquals(expected.getVersion(), actual.getVersion());
    assertEquals(expected.getRowGroupSize(), actual.getRowGroupSize());
    assertEquals(expected.getDataPageSize(), actual.getDataPageSize());
    assertEquals(expected.getDictionaryPageSize(), actual.getDictionaryPageSize());
    assertEquals(expected.getFields().size(), actual.getFields().size());
    for (int i = 0; i < expected.getFields().size(); i++) {
      assertEquals(
          expected.getFields().get(i).getSourceFieldName(),
          actual.getFields().get(i).getSourceFieldName());
      assertEquals(
          expected.getFields().get(i).getTargetFieldName(),
          actual.getFields().get(i).getTargetFieldName());
    }
  }
}
