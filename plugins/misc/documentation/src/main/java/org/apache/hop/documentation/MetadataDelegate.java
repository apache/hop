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
 *
 */

package org.apache.hop.documentation;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;

@Getter
@Setter
public class MetadataDelegate {
  public static final String METADATA_FOLDER = "metadata";
  public static final String TOP_METADATA_FILENAME = "index.md";
  private final DocBuilder docBuilder;

  MetadataDelegate(DocBuilder docBuilder) {
    this.docBuilder = docBuilder;
  }

  /**
   * Build a metadata sub-folder and even deeper sub-folders per found metadata type and one file
   * per element.
   *
   * @param toc The table of contents to update
   * @param targetRootFolder The target folder
   * @throws Exception In case anything goes wrong
   */
  public void documentMetadata(Toc toc, FileObject targetRootFolder) throws Exception {
    MultiMetadataProvider multiMetadataProvider = docBuilder.getMetadataProvider();

    docBuilder.getLog().logBasic("Documenting metadata in folder " + METADATA_FOLDER);

    FileObject metadataFolder = targetRootFolder.resolveFile(METADATA_FOLDER);
    if (!metadataFolder.exists()) {
      metadataFolder.createFolder();
    }

    StringBuilder topMetaPage = new StringBuilder();
    topMetaPage
        .append("---")
        .append(Const.CR)
        .append("title: Hop Metadata  ")
        .append(Const.CR)
        .append("---")
        .append(Const.CR)
        .append(Const.CR);

    topMetaPage.append("# Hop Metadata  ").append(Const.CR).append(Const.CR);

    // Create a top level file there to document the different metadata source folders
    //
    topMetaPage.append("## Sources  ").append(Const.CR);

    for (IHopMetadataProvider metadataProvider : multiMetadataProvider.getProviders()) {
      topMetaPage
          .append("- provider: ")
          .append(metadataProvider.getDescription())
          .append("  ")
          .append(Const.CR);
    }

    topMetaPage.append("## Elements  ").append(Const.CR);
    List<Class<IHopMetadata>> metadataClasses = multiMetadataProvider.getMetadataClasses();
    for (Class<IHopMetadata> metadataClass : metadataClasses) {
      IHopMetadataSerializer<IHopMetadata> serializer =
          multiMetadataProvider.getSerializer(metadataClass);
      List<String> names = serializer.listObjectNames();
      if (!names.isEmpty()) {
        topMetaPage
            .append("### ")
            .append(serializer.getDescription())
            .append("  ")
            .append(Const.CR);

        // List the names
        for (String name : names) {
          topMetaPage.append("* ").append(name).append("  ").append(Const.CR);
        }
        topMetaPage.append(Const.CR);
      }
    }

    // Now write this top metadata page
    //
    String topMetaFilename = metadataFolder + "/" + TOP_METADATA_FILENAME;
    docBuilder.saveFile(topMetaFilename, topMetaPage.toString());

    // Add it to the TOC index
    toc.getEntries()
        .add(
            new TocEntry(
                ".", "Metadata", "Overview", "", METADATA_FOLDER + "/" + TOP_METADATA_FILENAME));
  }
}
