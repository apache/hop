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
 *
 */

package org.apache.hop.vfs.gs;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Set;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.vfs.gs.config.GoogleCloudConfig;
import org.apache.hop.vfs.gs.config.GoogleCloudConfigSingleton;
import org.apache.hop.vfs.gs.metadatatype.GoogleStorageMetadataType;

public class GoogleStorageFileProvider extends AbstractOriginatingFileProvider {
  private FileSystemOptions newFileSystemOptions = new FileSystemOptions();

  public GoogleStorageFileProvider() {
    super();
    setServiceAccountCredentials(null, null);
  }

  public GoogleStorageFileProvider(
      IVariables variables, GoogleStorageMetadataType googleStorageMetadataType) {
    super();
    setServiceAccountCredentials(variables, googleStorageMetadataType);
  }

  // APPEND_CONTENT is required for append: without it commons-vfs2 rejects getOutputStream(true)
  // with "does not support append mode". Append itself is emulated with composite objects, see
  // ComposeAppendOutputStream.
  public static final Collection<Capability> capabilities =
      Set.of(
          Capability.CREATE,
          Capability.DELETE,
          Capability.GET_TYPE,
          Capability.GET_LAST_MODIFIED,
          Capability.SET_LAST_MODIFIED_FILE,
          Capability.SET_LAST_MODIFIED_FOLDER,
          Capability.LIST_CHILDREN,
          Capability.READ_CONTENT,
          Capability.URI,
          Capability.WRITE_CONTENT,
          Capability.APPEND_CONTENT);

  @Override
  public Collection<Capability> getCapabilities() {
    return capabilities;
  }

  @Override
  protected FileSystem doCreateFileSystem(FileName rootName, FileSystemOptions fileSystemOptions)
      throws FileSystemException {
    return new GoogleStorageFileSystem(rootName, null, newFileSystemOptions);
  }

  private void setServiceAccountCredentials(
      IVariables variables, GoogleStorageMetadataType googleStorageMetadataType) {
    try {
      GoogleCredentials credentials = null;
      String scheme = "gs";

      if (variables == null && googleStorageMetadataType == null) {
        // Default configuration: prefer explicit key file; ADC only as optional fallback.
        // Do not call ADC first — on non-GCP clusters (e.g. Databricks AWS) it probes metadata
        // and can throw NoClassDefFoundError for io.grpc.Context when gRPC is not on the fat jar.
        GoogleCloudConfig config = GoogleCloudConfigSingleton.getConfig();
        if (!StringUtils.isEmpty(config.getServiceAccountKeyFile())) {
          credentials =
              ServiceAccountCredentials.fromStream(
                  new FileInputStream(config.getServiceAccountKeyFile()));
        } else {
          credentials = tryApplicationDefaultCredentials();
        }
      } else {
        scheme = googleStorageMetadataType.getName();
        switch (googleStorageMetadataType.getStorageCredentialsType()) {
          case KEY_FILE:
            credentials =
                ServiceAccountCredentials.fromStream(
                    new FileInputStream(
                        variables.resolve(googleStorageMetadataType.getStorageAccountKey())));
            break;
          case KEY_STRING:
            credentials =
                ServiceAccountCredentials.fromStream(
                    IOUtils.toInputStream(
                        variables.resolve(googleStorageMetadataType.getStorageAccountKey()),
                        StandardCharsets.UTF_8));
            break;
          default:
            credentials = tryApplicationDefaultCredentials();
            break;
        }
      }
      if (credentials != null) {
        GoogleStorageFileSystemConfigBuilder.getInstance()
            .setGoogleCredentials(newFileSystemOptions, credentials);
      }
      GoogleStorageFileSystemConfigBuilder.getInstance().setSchema(newFileSystemOptions, scheme);
    } catch (Exception e) {
      // Do not log error for the default GS account
      if (googleStorageMetadataType != null) {
        LogChannel.GENERAL.logError(
            "Unable to set service account credentials for vfs name: "
                + googleStorageMetadataType.getName(),
            e);
      }
    } catch (LinkageError e) {
      // NoClassDefFoundError (e.g. io.grpc.Context) must not prevent HopEnvironment.init
      if (googleStorageMetadataType != null) {
        LogChannel.GENERAL.logError(
            "Unable to set service account credentials for vfs name: "
                + googleStorageMetadataType.getName()
                + " (missing dependency: "
                + e.getMessage()
                + ")",
            e);
      } else {
        LogChannel.GENERAL.logDetailed(
            "Google Storage VFS: Application Default Credentials unavailable ("
                + e.getClass().getSimpleName()
                + ": "
                + e.getMessage()
                + "). gs:// will work after configuring a service account key.");
      }
    }
  }

  /**
   * Best-effort ADC. Returns null if unavailable (not on GCE/GCP, or gRPC/auth deps missing from
   * classpath — common with native-provided fat jars on Databricks/AWS).
   */
  static GoogleCredentials tryApplicationDefaultCredentials() {
    try {
      return GoogleCredentials.getApplicationDefault();
    } catch (Exception e) {
      LogChannel.GENERAL.logDetailed(
          "Google Storage VFS: Application Default Credentials not available: " + e.getMessage());
      return null;
    } catch (LinkageError e) {
      LogChannel.GENERAL.logDetailed(
          "Google Storage VFS: Application Default Credentials not available ("
              + e.getClass().getSimpleName()
              + ": "
              + e.getMessage()
              + ")");
      return null;
    }
  }
}
