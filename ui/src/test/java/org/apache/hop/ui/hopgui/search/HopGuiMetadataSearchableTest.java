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

package org.apache.hop.ui.hopgui.search;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A metadata search result points at the file of the object. Project search hands it a
 * multi-provider's serializer, which knows no files: the file has to come from the provider which
 * holds the object (issue #5597).
 */
class HopGuiMetadataSearchableTest {

  /** A metadata type which was renamed: its objects can still be in the SearchType folder. */
  @HopMetadata(name = "Search type", key = "search-type", legacyKeys = "SearchType")
  public static class SearchType extends HopMetadataBase implements IHopMetadata {
    public SearchType() {
      // For the serializer.
    }
  }

  @TempDir Path parentFolder;
  @TempDir Path childFolder;

  @Test
  @SuppressWarnings("unchecked")
  void theFileOfAnObjectFoundThroughAMultiProvider() throws Exception {
    // The parent migrated to the current key, the child still has the object in the legacy folder
    // and wins, the way load() resolves it.
    write(parentFolder, "search-type", "api");
    write(childFolder, "SearchType", "api");

    HopTwoWayPasswordEncoder encoder = new HopTwoWayPasswordEncoder();
    MultiMetadataProvider multi =
        new MultiMetadataProvider(
            encoder,
            new ArrayList<IHopMetadataProvider>(
                List.of(
                    new JsonMetadataProvider(
                        encoder, parentFolder.toString(), Variables.getADefaultVariableSpace()),
                    new JsonMetadataProvider(
                        encoder, childFolder.toString(), Variables.getADefaultVariableSpace()))),
            Variables.getADefaultVariableSpace());
    IHopMetadataSerializer<IHopMetadata> serializer =
        (IHopMetadataSerializer<IHopMetadata>)
            (IHopMetadataSerializer<?>) multi.getSerializer(SearchType.class);

    HopGuiMetadataSearchable searchable =
        new HopGuiMetadataSearchable(
            multi,
            serializer,
            serializer.load("api"),
            (Class<IHopMetadata>) (Class<?>) SearchType.class);

    assertEquals(
        childFolder.resolve("SearchType").resolve("api.json").toRealPath(),
        Path.of(HopVfs.getFileObject(searchable.getFilename()).getURL().toURI()).toRealPath());
  }

  private static void write(Path metadataFolder, String key, String name) throws Exception {
    Path typeFolder = metadataFolder.resolve(key);
    Files.createDirectories(typeFolder);
    Files.write(
        typeFolder.resolve(name + ".json"), ("{\"name\":\"" + name + "\"}").getBytes(UTF_8));
  }
}
