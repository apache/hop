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

package org.apache.hop.vfs.gs;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.StorageRetryStrategy;
import com.google.storage.control.v2.StorageControlClient;
import com.google.storage.control.v2.StorageControlSettings;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.vfs.gs.config.GoogleCloudConfig;
import org.apache.hop.vfs.gs.config.GoogleCloudConfigSingleton;
import org.threeten.bp.Duration;

public class GoogleStorageFileSystem extends AbstractFileSystem {

  /**
   * Volatile, and set after {@link #stallLimits}: a thread that finds the client built by another
   * thread must also find the limits that go with it.
   */
  volatile Storage storage = null;

  /** When to report or end a transfer, from the same settings {@link #storage} was built with. */
  private GoogleStorageStallWatchdog.Limits stallLimits;

  StorageControlClient storageControlClient = null;
  FileSystemOptions fileSystemOptions;

  private GoogleStorageListCache listCache;

  private final AtomicBoolean credentialsProblemLogged = new AtomicBoolean();

  protected GoogleStorageFileSystem(
      FileName rootName, FileObject parentLayer, FileSystemOptions fileSystemOptions)
      throws FileSystemException {
    super(rootName, parentLayer, fileSystemOptions);
    this.fileSystemOptions = fileSystemOptions;
  }

  private GoogleStorageListCache getListCache() {
    if (listCache == null) {
      GoogleCloudConfig config = GoogleCloudConfigSingleton.getConfig();
      long ttlMs = org.apache.hop.core.Const.toLong(config.getCacheTtlSeconds(), 10L) * 1000L;
      listCache = new GoogleStorageListCache(ttlMs);
    }
    return listCache;
  }

  @Override
  protected FileObject createFile(AbstractFileName name) throws Exception {
    return new GoogleStorageFileObject(
        GoogleStorageFileSystemConfigBuilder.getInstance().getSchema(fileSystemOptions),
        name,
        this);
  }

  @Override
  protected void addCapabilities(Collection<Capability> caps) {
    caps.addAll(GoogleStorageFileProvider.capabilities);
  }

  Storage setupStorage() throws IOException {
    if (storage != null) {
      return storage;
    }

    GoogleCloudConfig config = GoogleCloudConfigSingleton.getConfig();

    StorageOptions.Builder optionsBuilder = buildStorageOptions(config);
    optionsBuilder.setCredentials(credentials());

    stallLimits = GoogleStorageStallWatchdog.Limits.from(config);
    return storage = optionsBuilder.build().getService();
  }

  /**
   * Assemble everything about the client that depends only on the configuration. Kept separate from
   * {@link #setupStorage()} - which additionally needs credentials and a live service - so the
   * wiring can be exercised from a test against a local endpoint.
   */
  static StorageOptions.Builder buildStorageOptions(GoogleCloudConfig config) {
    return buildStorageOptions(
        config, new LoggingStorageRetryStrategy(selectRetryStrategy(config), config));
  }

  /** As {@link #buildStorageOptions(GoogleCloudConfig)}, reporting retries to the given log. */
  static StorageOptions.Builder buildStorageOptions(
      GoogleCloudConfig config, LoggingStorageRetryStrategy.RetryLog retryLog) {
    return buildStorageOptions(
        config, new LoggingStorageRetryStrategy(selectRetryStrategy(config), config, retryLog));
  }

  private static StorageOptions.Builder buildStorageOptions(
      GoogleCloudConfig config, StorageRetryStrategy retryStrategy) {
    return StorageOptions.newBuilder()
        .setRetrySettings(buildRetrySettings(config))
        .setTransportOptions(buildTransportOptions(config))
        .setStorageRetryStrategy(retryStrategy);
  }

  /**
   * The GCS client only retries calls it considers idempotent. Object create, delete and
   * upload-session-start carry no preconditions here, so they are classified non-idempotent and are
   * never retried - whatever the configured number of attempts says. The uniform strategy drops
   * that distinction and retries writes too; see {@link
   * GoogleCloudConfig#getRetryNonIdempotentOperations()} for why it is opt-in.
   */
  static StorageRetryStrategy selectRetryStrategy(GoogleCloudConfig config) {
    return Boolean.TRUE.equals(config.getRetryNonIdempotentOperations())
        ? StorageRetryStrategy.getUniformStorageRetryStrategy()
        : StorageRetryStrategy.getDefaultStorageRetryStrategy();
  }

  static RetrySettings buildRetrySettings(GoogleCloudConfig config) {
    long initialRpcTimeout = Const.toLong(config.getInitialRpcTimeout(), 50);
    // gax rejects a max RPC timeout below the initial one with an IllegalStateException while the
    // client is being built, taking down all GCS access. Raise the ceiling to whatever the user
    // explicitly asked for as a starting point rather than failing to connect at all.
    long maxRpcTimeout = Math.max(Const.toLong(config.getMaxRpcTimeout(), 50), initialRpcTimeout);

    return StorageOptions.getDefaultRetrySettings().toBuilder()
        .setMaxAttempts(Const.toInt(config.getMaxAttempts(), 6))
        .setInitialRetryDelay(Duration.ofSeconds(Const.toLong(config.getInitialRetryDelay(), 1)))
        .setRetryDelayMultiplier(Const.toDouble(config.getRetryDelayMultiplier(), 2.0))
        .setMaxRetryDelay(Duration.ofSeconds(Const.toLong(config.getMaxRetryDelay(), 32)))
        // Minutes, unlike every other duration here - kept that way so upgrading does not silently
        // shorten existing configurations by a factor of 60. The label spells out the unit.
        .setTotalTimeout(Duration.ofMinutes(Const.toLong(config.getTotalTimeout(), 50)))
        .setInitialRpcTimeout(Duration.ofSeconds(initialRpcTimeout))
        .setRpcTimeoutMultiplier(Const.toDouble(config.getRpcTimeoutMultiplier(), 1.0))
        .setMaxRpcTimeout(Duration.ofSeconds(maxRpcTimeout))
        .build();
  }

  /**
   * The limits are taken from the settings the client was built with, so that the watchdog and the
   * client it watches always agree on the timeouts, even after the settings changed.
   *
   * @return how long a read or write may go without progress before it is reported or ended
   */
  GoogleStorageStallWatchdog.Limits stallLimits() throws IOException {
    setupStorage();
    return stallLimits;
  }

  static HttpTransportOptions buildTransportOptions(GoogleCloudConfig config) {
    int connectTimeoutMs = Const.toInt(config.getConnectionTimeout(), 20) * 1000;
    int readTimeoutMs = Const.toInt(config.getReadTimeout(), 20) * 1000;
    return HttpTransportOptions.newBuilder()
        .setConnectTimeout(connectTimeoutMs)
        .setReadTimeout(readTimeoutMs)
        .build();
  }

  String getBucketName(FileName name) {

    String path = name.getPath();
    int idx = path.indexOf('/', 1);
    if (idx > -1) {
      return name.getPath().substring(1, idx);
    } else {
      return name.getPath().substring(1);
    }
  }

  void putListCache(
      String bucket,
      String prefix,
      java.util.Map<String, GoogleStorageListCache.ChildInfo> entries) {
    getListCache().put(bucket, prefix, entries);
  }

  GoogleStorageListCache.ChildInfo getFromListCache(
      String bucket, String parentPrefix, String childFullKey) {
    return getListCache().get(bucket, parentPrefix, childFullKey);
  }

  void invalidateListCache(String bucket, String prefix) {
    getListCache().invalidate(bucket, prefix);
  }

  void invalidateListCacheForParentOf(String bucket, String key) {
    getListCache().invalidateParentOf(bucket, key);
  }

  String getBucketPath(FileName name) {
    int idx = name.getPath().indexOf('/', 1);
    if (idx > -1) {
      return name.getPath().substring(idx + 1);
    } else {
      return "";
    }
  }

  /**
   * The credentials the provider loaded. When it could not, the Google client would fail with a
   * bare NullPointerException; instead say why, once in the log and every time in the error.
   */
  private GoogleCredentials credentials() throws IOException {
    GoogleStorageFileSystemConfigBuilder builder =
        GoogleStorageFileSystemConfigBuilder.getInstance();
    GoogleCredentials credentials = builder.getGoogleCredentials(fileSystemOptions);
    if (credentials != null) {
      return credentials;
    }
    String problem = builder.getCredentialsProblem(fileSystemOptions);
    String message =
        "Google Cloud Storage: no credentials to access "
            + Const.NVL(builder.getSchema(fileSystemOptions), "gs")
            + "://: "
            + Const.NVL(problem, "none were configured");
    if (!credentialsProblemLogged.getAndSet(true)) {
      LogChannel.GENERAL.logError(message);
    }
    throw new IOException(message);
  }

  StorageControlClient getStorageControlClient() throws IOException {
    if (storageControlClient != null) {
      return storageControlClient;
    }
    RetrySettings retrySettings = buildRetrySettings(GoogleCloudConfigSingleton.getConfig());
    StorageControlSettings.Builder builder =
        StorageControlSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(credentials()));
    // This client was left on the library defaults, so the configured retry behaviour never
    // reached HNS folder operations.
    builder.applyToAllUnaryMethods(
        callSettings -> {
          callSettings.setRetrySettings(retrySettings);
          return null;
        });
    StorageControlSettings settings = builder.build();
    storageControlClient = StorageControlClient.create(settings);
    return storageControlClient;
  }
}
