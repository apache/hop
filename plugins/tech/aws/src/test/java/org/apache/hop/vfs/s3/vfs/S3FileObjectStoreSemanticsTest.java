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

package org.apache.hop.vfs.s3.vfs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.commons.vfs2.CacheStrategy;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.cache.SoftRefFilesCache;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.UriParser;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.vfs.s3.s3.vfs.S3FileProvider;
import org.apache.hop.vfs.s3.s3.vfs.S3FileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * The S3 provider against an in-memory bucket that answers like S3: keys are flat, a "folder" is
 * only a key prefix unless a marker object exists, and a missing key is a 404. Runs through a real
 * file system manager with the ON_RESOLVE cache strategy Hop uses.
 */
class S3FileObjectStoreSemanticsTest {

  private static final String BUCKET = "bucket";

  /** The bucket's objects: key to content. */
  private final TreeMap<String, byte[]> objects = new TreeMap<>();

  private DefaultFileSystemManager manager;

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    S3Client client = inMemoryClient();
    manager = new DefaultFileSystemManager();
    manager.setFilesCache(new SoftRefFilesCache());
    manager.setCacheStrategy(CacheStrategy.ON_RESOLVE);
    manager.addProvider(
        "s3",
        new S3FileProvider() {
          @Override
          public FileSystem doCreateFileSystem(FileName name, FileSystemOptions options) {
            return new S3FileSystem(name, options) {
              @Override
              public S3Client getS3Client() {
                return client;
              }
            };
          }
        });
    manager.init();
  }

  @AfterEach
  void tearDown() {
    manager.close();
  }

  private void put(String key, String content) {
    objects.put(key, content.getBytes(StandardCharsets.UTF_8));
  }

  private FileObject resolve(String path) throws FileSystemException {
    return manager.resolveFile("s3://" + BUCKET + "/" + path);
  }

  private static List<String> names(FileObject[] children) throws FileSystemException {
    List<String> names = new ArrayList<>();
    for (FileObject child : children) {
      // Base names keep VFS's %nn escapes, as they do for local files
      names.add(UriParser.decode(child.getName().getBaseName()));
    }
    return names;
  }

  private static NoSuchKeyException notFound() {
    return (NoSuchKeyException)
        NoSuchKeyException.builder()
            .statusCode(404)
            .awsErrorDetails(AwsErrorDetails.builder().errorCode("NoSuchKey").build())
            .build();
  }

  private S3Client inMemoryClient() {
    S3Client client = mock(S3Client.class);
    when(client.headObject(any(HeadObjectRequest.class)))
        .thenAnswer(
            call -> {
              byte[] content = objects.get(call.<HeadObjectRequest>getArgument(0).key());
              if (content == null) {
                throw notFound();
              }
              return HeadObjectResponse.builder().contentLength((long) content.length).build();
            });
    when(client.getObject(any(GetObjectRequest.class)))
        .thenAnswer(
            call -> {
              byte[] content = objects.get(call.<GetObjectRequest>getArgument(0).key());
              if (content == null) {
                throw notFound();
              }
              return new ResponseInputStream<>(
                  GetObjectResponse.builder().contentLength((long) content.length).build(),
                  AbortableInputStream.create(new ByteArrayInputStream(content)));
            });
    when(client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenAnswer(call -> list(call.getArgument(0)));
    return client;
  }

  /** ListObjectsV2 with a delimiter: direct objects as contents, deeper keys as common prefixes. */
  private ListObjectsV2Response list(ListObjectsV2Request request) {
    String prefix = request.prefix() == null ? "" : request.prefix();
    List<S3Object> contents = new ArrayList<>();
    TreeSet<String> prefixes = new TreeSet<>();
    for (String key : objects.keySet()) {
      if (!key.startsWith(prefix)) {
        continue;
      }
      int slash = key.indexOf('/', prefix.length());
      if (slash >= 0) {
        prefixes.add(key.substring(0, slash + 1));
      } else {
        contents.add(S3Object.builder().key(key).size((long) objects.get(key).length).build());
      }
    }
    return ListObjectsV2Response.builder()
        .contents(contents)
        .commonPrefixes(
            prefixes.stream().map(p -> CommonPrefix.builder().prefix(p).build()).toList())
        .build();
  }

  @Test
  void prefixFolderStaysAFolderAfterItsParentIsListedAgain() throws Exception {
    put("data/sub/file.txt", "x");

    FileObject data = resolve("data");
    assertEquals(List.of("sub"), names(data.getChildren()));
    FileObject sub = resolve("data/sub");
    assertEquals(FileType.FOLDER, sub.getType());

    data.getChildren();
    FileObject again = resolve("data/sub");

    assertEquals(FileType.FOLDER, again.getType());
    assertEquals(List.of("file.txt"), names(again.getChildren()));
  }

  @Test
  void prefixFolderStaysAFolderAfterARefresh() throws Exception {
    put("data/sub/file.txt", "x");
    resolve("data").getChildren();
    FileObject sub = resolve("data/sub");
    assertEquals(FileType.FOLDER, sub.getType());

    sub.refresh();

    assertEquals(FileType.FOLDER, sub.getType());
  }

  @Test
  void listsAndReadsAnObjectWithAPercentInItsName() throws Exception {
    put("data/file%.txt", "percent");
    put("data/100% sure/inner.txt", "inner");

    FileObject data = resolve("data");
    assertEquals(
        new TreeSet<>(List.of("file%.txt", "100% sure")), new TreeSet<>(names(data.getChildren())));

    for (FileObject child : data.getChildren()) {
      if (child.getName().getBaseName().equals("file%25.txt")) {
        assertEquals(FileType.FILE, child.getType());
        try (InputStream in = child.getContent().getInputStream()) {
          assertEquals("percent", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
      } else {
        assertEquals(FileType.FOLDER, child.getType());
        assertEquals(List.of("inner.txt"), names(child.getChildren()));
      }
    }
  }

  @Test
  void resolvesAPercentNameByItsEscapedForm() throws Exception {
    put("data/file%.txt", "percent");

    FileObject file = resolve("data/file%25.txt");

    assertTrue(file.exists());
    assertEquals("percent", file.getContent().getString(StandardCharsets.UTF_8));
  }

  @Test
  void readingAFolderSaysItIsNotAFile() throws Exception {
    put("data/sub/file.txt", "x");
    FileObject sub = resolve("data/sub");

    FileSystemException e =
        assertThrows(FileSystemException.class, () -> sub.getContent().getInputStream());

    assertEquals("vfs.provider/read-not-file.error", e.getCode());
  }

  @Test
  void readingAMissingObjectSaysItIsNotAFile() throws Exception {
    FileObject missing = resolve("data/missing.txt");

    FileSystemException e =
        assertThrows(FileSystemException.class, () -> missing.getContent().getInputStream());

    assertEquals("vfs.provider/read-not-file.error", e.getCode());
  }

  @Test
  void listingAFileSaysItIsNotAFolder() throws Exception {
    put("data/plain.txt", "content");
    FileObject file = resolve("data/plain.txt");

    FileSystemException e = assertThrows(FileSystemException.class, file::getChildren);

    assertEquals("vfs.provider/list-children-not-folder.error", e.getCode());
  }

  @Test
  void stillReadsAPlainFile() throws Exception {
    put("data/plain.txt", "content");

    assertEquals(
        "content", resolve("data/plain.txt").getContent().getString(StandardCharsets.UTF_8));
    assertEquals(Arrays.asList("plain.txt"), names(resolve("data").getChildren()));
  }
}
