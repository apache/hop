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

package org.apache.hop.metadata.serializer.json;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.serializer.BaseMetadataProvider;
import org.apache.hop.metadata.serializer.MetadataGuiFlows;
import org.apache.hop.metadata.serializer.json.occupation.Occupation;
import org.apache.hop.metadata.serializer.json.renamed.CaseRenamedType;
import org.apache.hop.metadata.serializer.json.renamed.RenamedType;
import org.apache.hop.metadata.serializer.json.renamed.TwiceRenamedType;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A metadata type which is renamed keeps its old key in {@link HopMetadata#legacyKeys()}: objects
 * in the old folder are still loaded, and saving them moves them to the new folder (issue #5597).
 *
 * <p>Most tests use a "mixed" project, which is what users have while a project migrates: some
 * objects only in the legacy folder, some only in the current folder and some in both.
 */
class JsonMetadataLegacyKeysTest {

  @TempDir Path folder;

  private JsonMetadataProvider provider;
  private JsonMetadataSerializer<RenamedType> serializer;
  private Path legacyFolder;
  private Path currentFolder;

  @BeforeEach
  void setUp() throws Exception {
    provider =
        new JsonMetadataProvider(
            new HopTwoWayPasswordEncoder(),
            folder.toString(),
            Variables.getADefaultVariableSpace());
    serializer = (JsonMetadataSerializer<RenamedType>) provider.getSerializer(RenamedType.class);
    legacyFolder = folder.resolve("RenamedType");
    currentFolder = folder.resolve("renamed-type");
  }

  private static void writeObject(Path typeFolder, String name, String description)
      throws Exception {
    Files.createDirectories(typeFolder);
    Files.write(
        typeFolder.resolve(name + ".json"),
        ("{\"name\":\"" + name + "\",\"description\":\"" + description + "\"}").getBytes(UTF_8));
  }

  /**
   * Writes an object the way an older version of Hop did: in the folder named after the old key.
   */
  private void writeLegacyObject(String name, String description) throws Exception {
    writeObject(legacyFolder, name, description);
  }

  private void writeCurrentObject(String name, String description) throws Exception {
    writeObject(currentFolder, name, description);
  }

  /** legacy-only: only in the old folder, current-only: only in the new one, both: in both. */
  private void writeMixedProject() throws Exception {
    writeLegacyObject("legacy-only", "legacy");
    writeCurrentObject("current-only", "current");
    writeLegacyObject("both", "outdated legacy copy");
    writeCurrentObject("both", "current copy");
  }

  private boolean inLegacy(String name) {
    return Files.exists(legacyFolder.resolve(name + ".json"));
  }

  private boolean inCurrent(String name) {
    return Files.exists(currentFolder.resolve(name + ".json"));
  }

  private List<String> sortedNames() throws HopException {
    List<String> names = new ArrayList<>(serializer.listObjectNames());
    names.sort(String::compareTo);
    return names;
  }

  // ---------------------------------------------------------------------------------------------
  // Reading
  // ---------------------------------------------------------------------------------------------

  @Test
  void testLoadFromLegacyFolder() throws Exception {
    writeLegacyObject("old", "Saved by an older version");

    assertEquals(List.of("old"), serializer.listObjectNames());
    assertTrue(serializer.exists("old"));
    RenamedType loaded = serializer.load("old");
    assertNotNull(loaded);
    assertEquals("Saved by an older version", loaded.getDescription());
    assertEquals(1, serializer.loadAll().size());
    assertEquals(
        legacyFolder.resolve("old.json").toString(),
        Path.of(serializer.findFilename("old")).toString());

    // Only reading doesn't move anything.
    assertFalse(Files.exists(currentFolder));
  }

  @Test
  void testMixedProjectIsReadAsOne() throws Exception {
    writeMixedProject();

    assertEquals(List.of("both", "current-only", "legacy-only"), sortedNames());
    assertTrue(serializer.exists("legacy-only"));
    assertTrue(serializer.exists("current-only"));
    assertTrue(serializer.exists("both"));
    assertFalse(serializer.exists("nowhere"));
    assertNull(serializer.load("nowhere"));
    assertNull(serializer.findFilename("nowhere"));

    assertEquals("legacy", serializer.load("legacy-only").getDescription());
    assertEquals("current", serializer.load("current-only").getDescription());
    // The current folder wins over the legacy folder.
    assertEquals("current copy", serializer.load("both").getDescription());
    assertEquals(
        currentFolder.resolve("both.json").toString(),
        Path.of(serializer.findFilename("both")).toString());

    List<RenamedType> all = serializer.loadAll();
    assertEquals(3, all.size());
    assertTrue(all.stream().anyMatch(t -> "current copy".equals(t.getDescription())));
    assertFalse(all.stream().anyMatch(t -> "outdated legacy copy".equals(t.getDescription())));
  }

  /** The provider name is how a multi-provider knows where to save an object back to. */
  @Test
  void testObjectsFromLegacyFolderKnowTheirProvider() throws Exception {
    writeLegacyObject("old", "legacy");

    assertEquals(provider.getDescription(), serializer.load("old").getMetadataProviderName());
  }

  @Test
  void testReadVirtualPath() throws Exception {
    Files.createDirectories(legacyFolder);
    Files.write(
        legacyFolder.resolve("filed.json"),
        "{\"name\":\"filed\",\"virtualPath\":\"/legacy/path\"}".getBytes(UTF_8));
    Files.write(
        legacyFolder.resolve("both.json"),
        "{\"name\":\"both\",\"virtualPath\":\"/legacy/path\"}".getBytes(UTF_8));
    Files.createDirectories(currentFolder);
    Files.write(
        currentFolder.resolve("both.json"),
        "{\"name\":\"both\",\"virtualPath\":\"/current/path\"}".getBytes(UTF_8));

    assertEquals("/legacy/path", serializer.readVirtualPath("filed"));
    assertEquals("/current/path", serializer.readVirtualPath("both"));
    assertThrows(HopException.class, () -> serializer.readVirtualPath("nowhere"));
  }

  @Test
  void testNoFoldersAtAll() throws Exception {
    assertTrue(serializer.listObjectNames().isEmpty());
    assertTrue(serializer.loadAll().isEmpty());
    assertFalse(serializer.exists("anything"));

    // Reading never creates folders.
    assertFalse(Files.exists(currentFolder));
    assertFalse(Files.exists(legacyFolder));
  }

  @Test
  void testOnlyJsonFilesDirectlyInTheLegacyFolderAreObjects() throws Exception {
    writeLegacyObject("real", "legacy");
    Files.write(legacyFolder.resolve("notes.txt"), "not metadata".getBytes(UTF_8));
    Files.createDirectories(legacyFolder.resolve("sub"));
    writeObject(legacyFolder.resolve("sub"), "nested", "not listed");

    assertEquals(List.of("real"), serializer.listObjectNames());
  }

  @Test
  void testNamesWithSpacesAndDotsInLegacyFolder() throws Exception {
    writeLegacyObject("My connection v1.2", "legacy");

    assertEquals(List.of("My connection v1.2"), serializer.listObjectNames());
    assertEquals("legacy", serializer.load("My connection v1.2").getDescription());

    RenamedType loaded = serializer.load("My connection v1.2");
    serializer.save(loaded);
    assertTrue(inCurrent("My connection v1.2"));
    assertFalse(inLegacy("My connection v1.2"));
  }

  /** A broken legacy object shouldn't hide the others, while loading it by name still reports. */
  @Test
  void testCorruptLegacyObjectIsSkippedByLoadAll() throws Exception {
    writeMixedProject();
    Files.write(legacyFolder.resolve("corrupt.json"), "{ \"name\": ".getBytes(UTF_8));

    assertEquals(4, serializer.listObjectNames().size());
    assertEquals(3, serializer.loadAll().size());
    assertThrows(HopException.class, () -> serializer.load("corrupt"));
  }

  // ---------------------------------------------------------------------------------------------
  // Saving
  // ---------------------------------------------------------------------------------------------

  @Test
  void testSaveMovesObjectToCurrentFolder() throws Exception {
    writeLegacyObject("old", "Saved by an older version");

    RenamedType loaded = serializer.load("old");
    loaded.setDescription("Changed");
    serializer.save(loaded);

    assertTrue(inCurrent("old"));
    assertFalse(inLegacy("old"));
    assertEquals(List.of("old"), serializer.listObjectNames());
    assertEquals("Changed", serializer.load("old").getDescription());
  }

  @Test
  void testSavingOneObjectOnlyMovesThatObject() throws Exception {
    writeMixedProject();
    writeLegacyObject("other-legacy", "legacy");

    serializer.save(serializer.load("legacy-only"));

    assertTrue(inCurrent("legacy-only"));
    assertFalse(inLegacy("legacy-only"));
    // Untouched objects stay where they are.
    assertTrue(inLegacy("other-legacy"));
    assertFalse(inCurrent("other-legacy"));
    assertTrue(inLegacy("both"));
    assertEquals(List.of("both", "current-only", "legacy-only", "other-legacy"), sortedNames());
  }

  /** Saving an object in both folders removes the outdated legacy copy. */
  @Test
  void testSavingAnObjectInBothFoldersRemovesTheLegacyCopy() throws Exception {
    writeMixedProject();

    RenamedType both = serializer.load("both");
    both.setDescription("saved again");
    serializer.save(both);

    assertTrue(inCurrent("both"));
    assertFalse(inLegacy("both"));
    assertEquals("saved again", serializer.load("both").getDescription());
  }

  @Test
  void testNewObjectsGoToCurrentFolder() throws Exception {
    serializer.save(new RenamedType("new", "Brand new"));

    assertTrue(inCurrent("new"));
    assertFalse(Files.exists(legacyFolder));
  }

  @Test
  void testSavingACurrentObjectDoesNotTouchTheLegacyFolder() throws Exception {
    writeMixedProject();

    serializer.save(serializer.load("current-only"));

    assertTrue(inCurrent("current-only"));
    assertFalse(inLegacy("current-only"));
    assertTrue(inLegacy("legacy-only"));
  }

  @Test
  void testMigrateAWholeProjectBySavingEverything() throws Exception {
    writeMixedProject();
    writeLegacyObject("another", "legacy");

    for (RenamedType t : serializer.loadAll()) {
      serializer.save(t);
    }

    try (var files = Files.list(legacyFolder)) {
      assertEquals(0, files.count(), "the legacy folder is empty after saving everything");
    }
    assertEquals(List.of("another", "both", "current-only", "legacy-only"), sortedNames());
    assertEquals("current copy", serializer.load("both").getDescription());
  }

  // ---------------------------------------------------------------------------------------------
  // Deleting
  // ---------------------------------------------------------------------------------------------

  @Test
  void testDeleteRemovesAllCopies() throws Exception {
    writeMixedProject();

    RenamedType deleted = serializer.delete("both");

    // The object returned is the one the user saw: the current copy.
    assertEquals("current copy", deleted.getDescription());
    assertFalse(serializer.exists("both"));
    assertFalse(inCurrent("both"));
    assertFalse(inLegacy("both"));
    assertEquals(List.of("current-only", "legacy-only"), sortedNames());
  }

  @Test
  void testDeleteLegacyOnlyObject() throws Exception {
    writeLegacyObject("old", "legacy");

    serializer.delete("old");

    assertFalse(serializer.exists("old"));
    assertFalse(inLegacy("old"));
    // Deleting doesn't create the current folder as a side effect.
    assertFalse(Files.exists(currentFolder));
  }

  @Test
  void testDeleteUnknownObjectFails() {
    assertThrows(HopException.class, () -> serializer.delete("nowhere"));
  }

  // ---------------------------------------------------------------------------------------------
  // Renaming, as the GUI does it
  // ---------------------------------------------------------------------------------------------

  @Test
  void testRenameLegacyObjectInTree() throws Exception {
    writeMixedProject();

    assertTrue(MetadataGuiFlows.renameInTree(serializer, "legacy-only", "renamed"));

    assertTrue(inCurrent("renamed"));
    assertFalse(inLegacy("legacy-only"));
    assertFalse(inCurrent("legacy-only"));
    assertEquals("legacy", serializer.load("renamed").getDescription());
    assertEquals(List.of("both", "current-only", "renamed"), sortedNames());
  }

  @Test
  void testRenameObjectInBothFoldersInTree() throws Exception {
    writeMixedProject();

    assertTrue(MetadataGuiFlows.renameInTree(serializer, "both", "renamed"));

    // The renamed object is the current copy, and no stale copy of the old name is left behind to
    // reappear under the old name.
    assertEquals("current copy", serializer.load("renamed").getDescription());
    assertFalse(serializer.exists("both"));
    assertFalse(inLegacy("both"));
    assertFalse(inCurrent("both"));
  }

  @Test
  void testRenameLegacyObjectInEditor() throws Exception {
    writeMixedProject();

    RenamedType metadata = serializer.load("legacy-only");
    metadata.setName("renamed");
    metadata.setDescription("edited");
    MetadataGuiFlows.renameInEditor(serializer, metadata, "legacy-only");

    assertTrue(inCurrent("renamed"));
    assertFalse(serializer.exists("legacy-only"));
    assertEquals("edited", serializer.load("renamed").getDescription());
  }

  /** Saving an open editor without changing the name is a plain save: it migrates the object. */
  @Test
  void testSaveInEditorWithoutRename() throws Exception {
    writeLegacyObject("old", "legacy");

    RenamedType metadata = serializer.load("old");
    metadata.setDescription("edited");
    MetadataGuiFlows.renameInEditor(serializer, metadata, "old");

    assertTrue(inCurrent("old"));
    assertFalse(inLegacy("old"));
    assertEquals("edited", serializer.load("old").getDescription());
  }

  /** A name only used in the legacy folder is still taken: renaming onto it would lose data. */
  @Test
  void testRenameOntoNameInLegacyFolderIsRefused() throws Exception {
    writeMixedProject();

    assertFalse(MetadataGuiFlows.renameInTree(serializer, "current-only", "legacy-only"));
    RenamedType metadata = serializer.load("current-only");
    metadata.setName("legacy-only");
    assertThrows(
        HopException.class,
        () -> MetadataGuiFlows.renameInEditor(serializer, metadata, "current-only"));

    // Nothing changed.
    assertEquals("legacy", serializer.load("legacy-only").getDescription());
    assertEquals("current", serializer.load("current-only").getDescription());
    assertEquals(List.of("both", "current-only", "legacy-only"), sortedNames());
  }

  @Test
  void testRenameOntoNameInCurrentFolderIsRefused() throws Exception {
    writeMixedProject();

    assertFalse(MetadataGuiFlows.renameInTree(serializer, "legacy-only", "current-only"));
    assertTrue(inLegacy("legacy-only"));
    assertEquals("current", serializer.load("current-only").getDescription());
  }

  @Test
  void testRenameBackAndForth() throws Exception {
    writeLegacyObject("a", "legacy");

    assertTrue(MetadataGuiFlows.renameInTree(serializer, "a", "b"));
    assertTrue(MetadataGuiFlows.renameInTree(serializer, "b", "a"));

    assertEquals(List.of("a"), serializer.listObjectNames());
    assertTrue(inCurrent("a"));
    assertFalse(inLegacy("a"));
    assertEquals("legacy", serializer.load("a").getDescription());
  }

  // ---------------------------------------------------------------------------------------------
  // Duplicating, as the GUI does it
  // ---------------------------------------------------------------------------------------------

  @Test
  void testDuplicateLegacyObject() throws Exception {
    writeLegacyObject("conn", "legacy");

    String copy = MetadataGuiFlows.duplicate(serializer, "conn");

    assertEquals("conn 2", copy);
    assertTrue(inCurrent("conn 2"));
    assertEquals("legacy", serializer.load("conn 2").getDescription());
    // The original isn't touched: duplicating isn't editing it.
    assertTrue(inLegacy("conn"));
    assertFalse(inCurrent("conn"));
    assertEquals(List.of("conn", "conn 2"), sortedNames());
  }

  /** A copy name used in the legacy folder is taken, so the next number is used. */
  @Test
  void testDuplicateSkipsNamesTakenInEitherFolder() throws Exception {
    writeCurrentObject("conn", "current");
    writeLegacyObject("conn 2", "legacy copy");
    writeCurrentObject("conn 3", "current copy");

    String copy = MetadataGuiFlows.duplicate(serializer, "conn");

    assertEquals("conn 4", copy);
    assertEquals("current", serializer.load("conn 4").getDescription());
    assertEquals("legacy copy", serializer.load("conn 2").getDescription());
    assertTrue(inLegacy("conn 2"));
    assertEquals(List.of("conn", "conn 2", "conn 3", "conn 4"), sortedNames());
  }

  @Test
  void testDuplicateObjectInBothFoldersCopiesTheCurrentCopy() throws Exception {
    writeMixedProject();

    String copy = MetadataGuiFlows.duplicate(serializer, "both");

    assertEquals("current copy", serializer.load(copy).getDescription());
  }

  // ---------------------------------------------------------------------------------------------
  // Types renamed more than once, or only in case
  // ---------------------------------------------------------------------------------------------

  @Test
  void testTypeRenamedTwice() throws Exception {
    JsonMetadataSerializer<TwiceRenamedType> twice =
        (JsonMetadataSerializer<TwiceRenamedType>) provider.getSerializer(TwiceRenamedType.class);
    Path current = folder.resolve("twice-renamed");
    Path previous = folder.resolve("TwiceRenamed");
    Path original = folder.resolve("OriginalName");

    writeObject(original, "only-original", "original");
    writeObject(previous, "only-previous", "previous");
    writeObject(original, "previous-and-original", "original");
    writeObject(previous, "previous-and-original", "previous");
    writeObject(original, "everywhere", "original");
    writeObject(previous, "everywhere", "previous");
    writeObject(current, "everywhere", "current");

    List<String> names = new ArrayList<>(twice.listObjectNames());
    names.sort(String::compareTo);
    assertEquals(
        List.of("everywhere", "only-original", "only-previous", "previous-and-original"), names);

    // The current key wins, then the most recent legacy key.
    assertEquals("current", twice.load("everywhere").getDescription());
    assertEquals("previous", twice.load("previous-and-original").getDescription());
    assertEquals("original", twice.load("only-original").getDescription());

    // Saving removes the object from every legacy folder.
    twice.save(twice.load("everywhere"));
    twice.save(twice.load("only-original"));
    assertFalse(Files.exists(original.resolve("everywhere.json")));
    assertFalse(Files.exists(previous.resolve("everywhere.json")));
    assertFalse(Files.exists(original.resolve("only-original.json")));
    assertTrue(Files.exists(current.resolve("only-original.json")));

    // Deleting removes every copy.
    twice.delete("previous-and-original");
    assertFalse(twice.exists("previous-and-original"));
    assertFalse(Files.exists(original.resolve("previous-and-original.json")));
    assertFalse(Files.exists(previous.resolve("previous-and-original.json")));
  }

  /**
   * A key which only changed case is the same folder on a case-insensitive file system (macOS,
   * Windows). Saving must then never delete the file it just wrote. On a case-sensitive file system
   * the old copy stays behind but the current one wins, so either way the saved object is loaded.
   */
  @Test
  void testCaseOnlyRenameNeverLosesTheSavedObject() throws Exception {
    JsonMetadataSerializer<CaseRenamedType> caseSerializer =
        (JsonMetadataSerializer<CaseRenamedType>) provider.getSerializer(CaseRenamedType.class);
    writeObject(folder.resolve("Case-Renamed"), "obj", "old");

    CaseRenamedType loaded = caseSerializer.load("obj");
    assertEquals("old", loaded.getDescription());
    loaded.setDescription("saved");
    caseSerializer.save(loaded);

    assertTrue(Files.exists(folder.resolve("case-renamed").resolve("obj.json")));
    assertEquals("saved", caseSerializer.load("obj").getDescription());
    assertEquals(List.of("obj"), caseSerializer.listObjectNames());

    caseSerializer.delete("obj");
    assertFalse(caseSerializer.exists("obj"));
  }

  // ---------------------------------------------------------------------------------------------
  // Keys
  // ---------------------------------------------------------------------------------------------

  @Test
  void testAllKeysAndMatching() {
    HopMetadata annotation = RenamedType.class.getAnnotation(HopMetadata.class);

    assertEquals(List.of("renamed-type", "RenamedType"), HopMetadataUtil.getAllKeys(annotation));
    assertTrue(HopMetadataUtil.matchesKey(annotation, "renamed-type"));
    assertTrue(HopMetadataUtil.matchesKey(annotation, "RenamedType"));
    assertFalse(HopMetadataUtil.matchesKey(annotation, "renamedtype"));
    assertFalse(HopMetadataUtil.matchesKey(annotation, null));
    assertFalse(HopMetadataUtil.matchesKey(null, "renamed-type"));

    assertEquals(
        List.of("twice-renamed", "TwiceRenamed", "OriginalName"),
        HopMetadataUtil.getAllKeys(TwiceRenamedType.class.getAnnotation(HopMetadata.class)));
  }

  /** A type without legacy keys only has its own folder. */
  @Test
  void testTypeWithoutLegacyKeys() throws Exception {
    JsonMetadataSerializer<?> plain =
        (JsonMetadataSerializer<?>) provider.getSerializer(Occupation.class);
    assertTrue(plain.getLegacyFolders().isEmpty());
  }

  /** A serialized metadata export from an older version of Hop uses the legacy key. */
  @Test
  void testMetadataClassForLegacyKey() throws Exception {
    BaseMetadataProvider base =
        new BaseMetadataProvider(Variables.getADefaultVariableSpace(), "test") {
          @Override
          public <T extends IHopMetadata> List<Class<T>> getMetadataClasses() {
            return List.of(
                (Class<T>) (Class<?>) RenamedType.class,
                (Class<T>) (Class<?>) TwiceRenamedType.class);
          }
        };

    assertEquals(RenamedType.class, base.getMetadataClassForKey("RenamedType"));
    assertEquals(TwiceRenamedType.class, base.getMetadataClassForKey("TwiceRenamed"));
    assertEquals(TwiceRenamedType.class, base.getMetadataClassForKey("OriginalName"));
    assertThrows(HopException.class, () -> base.getMetadataClassForKey("NoSuchType"));
  }
}
