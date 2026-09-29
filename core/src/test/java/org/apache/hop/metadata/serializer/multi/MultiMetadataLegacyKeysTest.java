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

package org.apache.hop.metadata.serializer.multi;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.serializer.MetadataGuiFlows;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.apache.hop.metadata.serializer.json.renamed.RenamedType;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Renamed metadata types with several metadata folders: a parent project and a child project, or
 * the folders listed in {@code HOP_METADATA_FOLDER}. Each folder can be anywhere in its migration,
 * independently of the others (issue #5597).
 */
class MultiMetadataLegacyKeysTest {

  @TempDir Path parentFolder;
  @TempDir Path childFolder;

  private MultiMetadataProvider multi;
  private IHopMetadataSerializer<RenamedType> serializer;

  @BeforeEach
  void setUp() throws Exception {
    HopTwoWayPasswordEncoder encoder = new HopTwoWayPasswordEncoder();
    JsonMetadataProvider parent =
        new JsonMetadataProvider(
            encoder, parentFolder.toString(), Variables.getADefaultVariableSpace());
    JsonMetadataProvider child =
        new JsonMetadataProvider(
            encoder, childFolder.toString(), Variables.getADefaultVariableSpace());
    // Like a project with a parent project: the last provider is the child, which wins.
    multi =
        new MultiMetadataProvider(
            encoder,
            new ArrayList<IHopMetadataProvider>(List.of(parent, child)),
            Variables.getADefaultVariableSpace());
    serializer = multi.getSerializer(RenamedType.class);
  }

  private static void write(Path metadataFolder, String key, String name, String description)
      throws Exception {
    Path typeFolder = metadataFolder.resolve(key);
    Files.createDirectories(typeFolder);
    Files.write(
        typeFolder.resolve(name + ".json"),
        ("{\"name\":\"" + name + "\",\"description\":\"" + description + "\"}").getBytes(UTF_8));
  }

  private static boolean has(Path metadataFolder, String key, String name) {
    return Files.exists(metadataFolder.resolve(key).resolve(name + ".json"));
  }

  private static final String LEGACY = "RenamedType";
  private static final String CURRENT = "renamed-type";

  private List<String> sortedNames() throws HopException {
    List<String> names = new ArrayList<>(serializer.listObjectNames());
    names.sort(String::compareTo);
    return names;
  }

  @Test
  void testChildOverridesParentWhateverTheFolders() throws Exception {
    // Parent migrated, child didn't.
    write(parentFolder, CURRENT, "a", "parent");
    write(childFolder, LEGACY, "a", "child");
    // Parent didn't migrate, child did.
    write(parentFolder, LEGACY, "b", "parent");
    write(childFolder, CURRENT, "b", "child");

    assertEquals("child", serializer.load("a").getDescription());
    assertEquals("child", serializer.load("b").getDescription());
    assertEquals(List.of("a", "b"), sortedNames());
    assertEquals(2, serializer.loadAll().size());
    assertTrue(
        serializer.loadAll().stream().allMatch(t -> "child".equals(t.getDescription())),
        "loadAll gives the child's objects, like load");
  }

  /**
   * The file of an object is the one load() reads: the child project before the parent, the current
   * folder before the legacy one. That is what the metadata perspective opens and what a search
   * result points at.
   */
  @Test
  void testFilenameIsTheFileLoadReads() throws Exception {
    // Parent migrated, child didn't.
    write(parentFolder, CURRENT, "a", "parent");
    write(childFolder, LEGACY, "a", "child");
    // Parent didn't migrate, child did.
    write(parentFolder, LEGACY, "b", "parent");
    write(childFolder, CURRENT, "b", "child");
    // Only the parent has it, not migrated.
    write(parentFolder, LEGACY, "c", "parent");
    // The child has an outdated legacy copy next to the current one.
    write(childFolder, LEGACY, "d", "outdated");
    write(childFolder, CURRENT, "d", "child");

    assertFilename(childFolder, LEGACY, "a");
    assertFilename(childFolder, CURRENT, "b");
    assertFilename(parentFolder, LEGACY, "c");
    assertFilename(childFolder, CURRENT, "d");
    assertEquals("child", serializer.load("d").getDescription());
    assertEquals(null, HopMetadataUtil.findFilename(multi, RenamedType.class, "nowhere"));
  }

  private void assertFilename(Path metadataFolder, String key, String name) throws Exception {
    String filename = HopMetadataUtil.findFilename(multi, RenamedType.class, name);
    assertEquals(
        metadataFolder.resolve(key).resolve(name + ".json").toRealPath(),
        Path.of(HopVfs.getFileObject(filename).getURL().toURI()).toRealPath(),
        name);
  }

  @Test
  void testMixedFoldersInEveryProvider() throws Exception {
    write(parentFolder, LEGACY, "parent-legacy", "p");
    write(parentFolder, CURRENT, "parent-current", "p");
    write(childFolder, LEGACY, "child-legacy", "c");
    write(childFolder, CURRENT, "child-current", "c");

    assertEquals(
        List.of("child-current", "child-legacy", "parent-current", "parent-legacy"), sortedNames());
    for (String name : sortedNames()) {
      assertTrue(serializer.exists(name), name);
    }
    assertEquals(4, serializer.loadAll().size());
  }

  /** An object is saved back to the project it came from, and moves there. */
  @Test
  void testSaveStaysInTheOwningProvider() throws Exception {
    write(parentFolder, LEGACY, "shared", "parent");

    RenamedType shared = serializer.load("shared");
    shared.setDescription("edited");
    serializer.save(shared);

    assertTrue(has(parentFolder, CURRENT, "shared"));
    assertFalse(has(parentFolder, LEGACY, "shared"));
    assertFalse(has(childFolder, CURRENT, "shared"));
    assertFalse(has(childFolder, LEGACY, "shared"));
    assertEquals("edited", serializer.load("shared").getDescription());
  }

  @Test
  void testNewObjectGoesToTheChildsCurrentFolder() throws Exception {
    write(parentFolder, LEGACY, "existing", "parent");

    serializer.save(new RenamedType("new", "new"));

    assertTrue(has(childFolder, CURRENT, "new"));
    assertFalse(Files.exists(childFolder.resolve(LEGACY)));
    assertFalse(has(parentFolder, CURRENT, "new"));
  }

  @Test
  void testRenameInTreeInParentProject() throws Exception {
    write(parentFolder, LEGACY, "old", "parent");
    write(childFolder, CURRENT, "unrelated", "child");

    assertTrue(MetadataGuiFlows.renameInTree(serializer, "old", "new"));

    assertTrue(has(parentFolder, CURRENT, "new"));
    assertFalse(has(parentFolder, LEGACY, "old"));
    assertFalse(serializer.exists("old"));
    assertEquals("parent", serializer.load("new").getDescription());
    assertEquals(List.of("new", "unrelated"), sortedNames());
  }

  @Test
  void testRenameInEditorInChildProject() throws Exception {
    write(childFolder, LEGACY, "old", "child");

    RenamedType metadata = serializer.load("old");
    metadata.setName("new");
    MetadataGuiFlows.renameInEditor(serializer, metadata, "old");

    assertTrue(has(childFolder, CURRENT, "new"));
    assertFalse(has(childFolder, LEGACY, "old"));
    assertEquals(List.of("new"), sortedNames());
  }

  /** Names are unique over all the projects and all the folders. */
  @Test
  void testRenameOntoNameInOtherProjectsLegacyFolderIsRefused() throws Exception {
    write(parentFolder, LEGACY, "taken", "parent");
    write(childFolder, CURRENT, "mine", "child");

    assertFalse(MetadataGuiFlows.renameInTree(serializer, "mine", "taken"));
    assertEquals("parent", serializer.load("taken").getDescription());
    assertEquals("child", serializer.load("mine").getDescription());
  }

  /**
   * The copy is a new object of the active (child) project, in the current folder. It is not
   * written next to the original in the parent project, which would share it with every project
   * that inherits that parent.
   */
  @Test
  void testDuplicateInParentProjectGoesToTheChild() throws Exception {
    write(parentFolder, LEGACY, "conn", "parent");
    write(childFolder, LEGACY, "conn 2", "child");

    String copy = MetadataGuiFlows.duplicate(serializer, "conn");

    assertEquals("conn 3", copy);
    assertTrue(has(childFolder, CURRENT, "conn 3"));
    assertFalse(has(parentFolder, CURRENT, "conn 3"));
    assertFalse(has(parentFolder, LEGACY, "conn 3"));
    assertTrue(has(parentFolder, LEGACY, "conn"), "the original isn't moved");
    assertTrue(has(childFolder, LEGACY, "conn 2"));
    assertEquals(
        multi.getProviders().get(1).getDescription(),
        serializer.load(copy).getMetadataProviderName());
    assertEquals(List.of("conn", "conn 2", "conn 3"), sortedNames());
  }

  /** Accepting the dialog default passes the active project's provider description. */
  @Test
  void testDuplicateInParentProjectTargetsTheChildExplicitly() throws Exception {
    write(parentFolder, LEGACY, "conn", "parent");

    String childProvider = multi.getProviders().get(1).getDescription();
    String copy = MetadataGuiFlows.duplicate(serializer, "conn", childProvider);

    assertEquals("conn 2", copy);
    assertTrue(has(childFolder, CURRENT, "conn 2"));
    assertFalse(has(parentFolder, CURRENT, "conn 2"));
    assertTrue(has(parentFolder, LEGACY, "conn"), "the original isn't moved");
    assertEquals(childProvider, serializer.load(copy).getMetadataProviderName());
  }

  /** The duplicate dialog can still save the copy in the project that owns the original. */
  @Test
  void testDuplicateCanStayWithTheOriginal() throws Exception {
    write(parentFolder, LEGACY, "conn", "parent");
    write(childFolder, CURRENT, "other", "child");

    String parentProvider = multi.getProviders().get(0).getDescription();
    String copy = MetadataGuiFlows.duplicate(serializer, "conn", parentProvider);

    assertEquals("conn 2", copy);
    assertTrue(has(parentFolder, CURRENT, "conn 2"));
    assertFalse(has(childFolder, CURRENT, "conn 2"));
    assertTrue(has(parentFolder, LEGACY, "conn"), "the original isn't moved");
    assertEquals(parentProvider, serializer.load(copy).getMetadataProviderName());
  }

  @Test
  void testDuplicateOfLocalObjectStaysInTheChild() throws Exception {
    write(childFolder, CURRENT, "local", "child");

    String copy = MetadataGuiFlows.duplicate(serializer, "local");

    assertEquals("local 2", copy);
    assertTrue(has(childFolder, CURRENT, "local 2"));
    assertFalse(has(parentFolder, CURRENT, "local 2"));
    assertFalse(Files.exists(parentFolder.resolve(CURRENT)));
  }

  @Test
  void testDuplicateProviderChoices() {
    List<IHopMetadataProvider> providers = multi.getProviders();
    String parent = providers.get(0).getDescription();
    String child = providers.get(1).getDescription();

    assertEquals(List.of(), HopMetadataUtil.duplicateProviderChoices(multi, null));
    assertEquals(List.of(), HopMetadataUtil.duplicateProviderChoices(multi, ""));
    assertEquals(List.of(), HopMetadataUtil.duplicateProviderChoices(multi, child));
    assertEquals(List.of(), HopMetadataUtil.duplicateProviderChoices(providers.get(1), parent));
    assertEquals(List.of(child, parent), HopMetadataUtil.duplicateProviderChoices(multi, parent));
    assertEquals(
        List.of(child, parent),
        HopMetadataUtil.duplicateProviderChoices(multi, "JSON metadata in folder /missing"));
  }

  /**
   * Deleting an object the child overrides removes the child's copies only, in all its folders. The
   * parent's object then shows through again, as it did before legacy keys existed.
   */
  @Test
  void testDeleteOverriddenObject() throws Exception {
    write(parentFolder, LEGACY, "x", "parent");
    write(childFolder, LEGACY, "x", "child outdated");
    write(childFolder, CURRENT, "x", "child");

    RenamedType deleted = serializer.delete("x");

    assertEquals("child", deleted.getDescription());
    assertFalse(has(childFolder, CURRENT, "x"));
    assertFalse(has(childFolder, LEGACY, "x"));
    assertTrue(serializer.exists("x"));
    assertEquals("parent", serializer.load("x").getDescription());

    serializer.delete("x");
    assertFalse(serializer.exists("x"));
    assertThrows(HopException.class, () -> serializer.delete("x"));
  }

  @Test
  void testReadVirtualPathFollowsTheSameOrder() throws Exception {
    Path parentCurrent = parentFolder.resolve(CURRENT);
    Files.createDirectories(parentCurrent);
    Files.write(
        parentCurrent.resolve("v.json"),
        "{\"name\":\"v\",\"virtualPath\":\"/parent\"}".getBytes(UTF_8));
    Path childLegacy = childFolder.resolve(LEGACY);
    Files.createDirectories(childLegacy);
    Files.write(
        childLegacy.resolve("v.json"),
        "{\"name\":\"v\",\"virtualPath\":\"/child\"}".getBytes(UTF_8));

    assertEquals("/child", serializer.readVirtualPath("v"));
  }
}
