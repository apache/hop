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

package org.apache.hop.vfs.s3.s3common;

import java.net.URI;
import java.nio.file.Paths;
import java.util.Collection;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.vfs.s3.amazon.s3.S3Util;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public abstract class S3CommonFileSystem extends AbstractFileSystem {

  private String awsAccessKeyCache;
  private String awsSecretKeyCache;
  private S3Client client;

  /** Cache for list results to avoid headObject/list calls when resolving child type. */
  private final S3ListCache listCache;

  protected S3CommonFileSystem(final FileName rootName, final FileSystemOptions fileSystemOptions) {
    super(rootName, null, fileSystemOptions);
    long ttlMs = S3ListCache.DEFAULT_TTL_MS;
    if (fileSystemOptions != null) {
      S3CommonFileSystemConfigBuilder cfg = new S3CommonFileSystemConfigBuilder(fileSystemOptions);
      String cacheTtl = cfg.getCacheTtlSeconds();
      if (!S3Util.isEmpty(cacheTtl)) {
        try {
          ttlMs = Long.parseLong(cacheTtl) * 1000L;
        } catch (NumberFormatException e) {
          LogChannel.GENERAL.logError("Invalid cacheTtlSeconds value: " + cacheTtl, e);
        }
      }
    }
    this.listCache = new S3ListCache(ttlMs);
  }

  @Override
  protected void addCapabilities(Collection caps) {
    caps.addAll(S3CommonFileProvider.capabilities);
  }

  @Override
  protected abstract FileObject createFile(AbstractFileName name) throws Exception;

  public S3Client getS3Client() {
    if (client == null && getFileSystemOptions() != null) {
      S3CommonFileSystemConfigBuilder cfg =
          new S3CommonFileSystemConfigBuilder(getFileSystemOptions());
      String accessKey = cfg.getAccessKey();
      String secretKey = cfg.getSecretKey();
      String sessionToken = cfg.getSessionToken();
      String region = cfg.getRegion();
      String credentialsFilePath = cfg.getCredentialsFile();
      String profileName = cfg.getProfileName();
      String endpoint = cfg.getEndpoint();
      String pathStyleAccess = cfg.getPathStyleAccess();
      boolean pathStyle = (pathStyleAccess == null) || Boolean.parseBoolean(pathStyleAccess);

      AwsCredentialsProvider credentialsProvider = null;
      Region awsRegion = Region.US_EAST_1;

      if (!S3Util.isEmpty(accessKey) && !S3Util.isEmpty(secretKey)) {
        if (S3Util.isEmpty(sessionToken)) {
          credentialsProvider =
              StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
        } else {
          credentialsProvider =
              StaticCredentialsProvider.create(
                  AwsSessionCredentials.create(accessKey, secretKey, sessionToken));
        }
      } else if (!S3Util.isEmpty(credentialsFilePath)) {
        ProfileCredentialsProvider.Builder profileBuilder =
            ProfileCredentialsProvider.builder()
                .profileFile(
                    ProfileFile.builder()
                        .content(Paths.get(credentialsFilePath))
                        .type(ProfileFile.Type.CREDENTIALS)
                        .build())
                .profileName(S3Util.isEmpty(profileName) ? "default" : profileName);
        credentialsProvider = profileBuilder.build();
      } else if (!S3Util.isEmpty(profileName)) {
        credentialsProvider = ProfileCredentialsProvider.builder().profileName(profileName).build();
      }

      if (!S3Util.isEmpty(region)) {
        awsRegion = Region.of(region);
      }

      boolean crossRegion = S3Util.isEmpty(region);
      if (cfg.getUseAnonymousAccess()) {
        // Named connection with "Anonymous access": single client, no credentials
        S3ClientBuilder anonymousBuilder =
            S3Client.builder()
                .crossRegionAccessEnabled(crossRegion)
                .region(awsRegion)
                .credentialsProvider(AnonymousCredentialsProvider.create());
        if (!S3Util.isEmpty(endpoint)) {
          anonymousBuilder.endpointOverride(URI.create(endpoint)).forcePathStyle(pathStyle);
        }
        client = anonymousBuilder.build();
      } else if (credentialsProvider != null) {
        // Explicit credentials: single client
        S3ClientBuilder builder =
            S3Client.builder()
                .crossRegionAccessEnabled(crossRegion)
                .region(awsRegion)
                .credentialsProvider(credentialsProvider);

        if (!S3Util.isEmpty(endpoint)) {
          builder.endpointOverride(URI.create(endpoint)).forcePathStyle(pathStyle);
        }

        client = builder.build();
      } else {
        // Default client: use default credential chain first, fall back to anonymous on 403.
        // This keeps the behavior that worked in the past (public buckets work without
        // credentials).
        S3ClientBuilder baseBuilder =
            S3Client.builder().crossRegionAccessEnabled(crossRegion).region(awsRegion);

        if (!S3Util.isEmpty(endpoint)) {
          baseBuilder.endpointOverride(URI.create(endpoint)).forcePathStyle(pathStyle);
        }

        S3Client primaryClient =
            baseBuilder.credentialsProvider(DefaultCredentialsProvider.create()).build();

        S3ClientBuilder anonymousBuilder =
            S3Client.builder()
                .crossRegionAccessEnabled(crossRegion)
                .region(awsRegion)
                .credentialsProvider(AnonymousCredentialsProvider.create());
        if (!S3Util.isEmpty(endpoint)) {
          anonymousBuilder.endpointOverride(URI.create(endpoint)).forcePathStyle(pathStyle);
        }
        S3Client anonymousClient = anonymousBuilder.build();

        client = S3ClientWithAnonymousFallback.create(primaryClient, anonymousClient);
      }
    }
    if (client == null || hasClientChangedCredentials()) {
      try {
        // No options (e.g. plain s3://): default chain with anonymous fallback on 403
        String regionStr = System.getenv(S3Util.AWS_REGION);
        Region region = regionStr != null ? Region.of(regionStr) : Region.US_EAST_1;
        boolean crossRegion = S3Util.isEmpty(regionStr);
        S3Client primaryClient =
            S3Client.builder()
                .crossRegionAccessEnabled(crossRegion)
                .region(region)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
        S3Client anonymousClient =
            S3Client.builder()
                .crossRegionAccessEnabled(crossRegion)
                .region(region)
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .build();
        client = S3ClientWithAnonymousFallback.create(primaryClient, anonymousClient);
        awsAccessKeyCache = System.getProperty(S3Util.ACCESS_KEY_SYSTEM_PROPERTY);
        awsSecretKeyCache = System.getProperty(S3Util.SECRET_KEY_SYSTEM_PROPERTY);
      } catch (Throwable t) {
        LogChannel.GENERAL.logError("Could not get an S3Client", t);
      }
    }
    return client;
  }

  private boolean hasClientChangedCredentials() {
    return client != null
        && (S3Util.hasChanged(
                awsAccessKeyCache, System.getProperty(S3Util.ACCESS_KEY_SYSTEM_PROPERTY))
            || S3Util.hasChanged(
                awsSecretKeyCache, System.getProperty(S3Util.SECRET_KEY_SYSTEM_PROPERTY)));
  }

  /** Put list result into cache so child getType()/exists() can avoid network calls. */
  public void putListCache(
      String bucket, String prefix, java.util.Map<String, S3ListCache.ChildInfo> childEntries) {
    listCache.put(bucket, prefix, childEntries);
  }

  /** Get cached type/size/lastModified for a child, or null if not cached or expired. */
  public S3ListCache.ChildInfo getFromListCache(
      String bucket, String parentPrefix, String childFullKey) {
    return listCache.get(bucket, parentPrefix, childFullKey);
  }

  /** Invalidate list cache for the given prefix (e.g. after put/delete). */
  public void invalidateListCache(String bucket, String prefix) {
    listCache.invalidate(bucket, prefix);
  }

  /** Invalidate list cache for the parent directory of the given key. */
  public void invalidateListCacheForParentOf(String bucket, String key) {
    listCache.invalidateParentOf(bucket, key);
  }
}
