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

package org.apache.hop.vfs.azure;

import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.UserAuthenticationData;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;
import org.apache.commons.vfs2.util.UserAuthenticatorUtils;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.azure.config.AzureConfig;
import org.apache.hop.vfs.azure.config.AzureConfigSingleton;
import org.apache.hop.vfs.azure.metadatatype.AzureMetadataType;

public class AzureFileProvider extends AbstractOriginatingFileProvider {

  public static final Collection<Capability> capabilities =
      Collections.unmodifiableCollection(
          Arrays.asList(
              Capability.CREATE,
              Capability.DELETE,
              Capability.RENAME,
              Capability.ATTRIBUTES,
              Capability.GET_TYPE,
              Capability.GET_LAST_MODIFIED,
              Capability.LIST_CHILDREN,
              Capability.READ_CONTENT,
              Capability.URI,
              Capability.WRITE_CONTENT));

  public static final UserAuthenticationData.Type[] AUTHENTICATOR_TYPES =
      new UserAuthenticationData.Type[] {
        UserAuthenticationData.USERNAME, UserAuthenticationData.PASSWORD
      };

  public static final String AZURE_ENDPOINT_SUFFIX = "core.windows.net";

  private static final FileSystemOptions defaultOptions = new FileSystemOptions();

  public static FileSystemOptions getDefaultFileSystemOptions() {
    return defaultOptions;
  }

  private final Log logger = LogFactory.getLog(AzureFileProvider.class);

  private IVariables variables;

  private AzureMetadataType azureMetadataType;

  public AzureFileProvider() {
    super();
    setFileNameParser(AzureFileNameParser.getInstance());
  }

  public AzureFileProvider(IVariables variables, AzureMetadataType azureMetadataType) {
    super();
    this.variables = variables;
    this.azureMetadataType = azureMetadataType;
    setFileNameParser(new AzureCustomFileNameParser(azureMetadataType.getName()));
  }

  @Override
  protected FileSystem doCreateFileSystem(FileName fileName, FileSystemOptions fileSystemOptions)
      throws FileSystemException {

    FileSystemOptions fsOptions =
        fileSystemOptions != null ? fileSystemOptions : getDefaultFileSystemOptions();

    UserAuthenticationData authData = null;
    AzureFileSystem azureFileSystem;
    String account;
    String key;
    String endpoint;

    try {
      authData = UserAuthenticatorUtils.authenticate(fsOptions, AUTHENTICATOR_TYPES);

      logger.info("Initialize Azure client");

      AzureFileName azureRootName = (AzureFileName) fileName;
      if (azureMetadataType != null) {

        if (StringUtils.isEmpty(azureMetadataType.getStorageAccountName())) {
          throw new FileSystemException(
              "Azure configuration \""
                  + azureMetadataType.getName()
                  + "\" is missing a storage account name");
        }
        if (StringUtils.isEmpty(azureMetadataType.getStorageAccountKey())) {
          throw new FileSystemException(
              "Azure configuration \""
                  + azureMetadataType.getName()
                  + "\" is missing a storage account key");
        }

        account = variables.resolve(azureMetadataType.getStorageAccountName());
        key =
            Encr.decryptPasswordOptionallyEncrypted(
                variables.resolve(azureMetadataType.getStorageAccountKey()));
        endpoint =
            (!Utils.isEmpty(azureMetadataType.getStorageAccountEndpoint()))
                ? variables.resolve(azureMetadataType.getStorageAccountEndpoint())
                : String.format(Locale.ROOT, "https://%s.dfs.core.windows.net", account);
      } else {
        AzureConfig config = AzureConfigSingleton.getConfig();

        if (StringUtils.isEmpty(config.getAccount())) {
          throw new FileSystemException(
              "Please configure the Azure account to use in the configuration (Options dialog or with hop-conf)");
        }
        if (StringUtils.isEmpty(config.getKey())) {
          throw new FileSystemException(
              "Please configure the Azure key to use in the configuration (Options dialog or with hop-conf)");
        }

        IVariables newVariables = Variables.getADefaultVariableSpace();
        account = newVariables.resolve(config.getAccount());
        key = Encr.decryptPasswordOptionallyEncrypted(newVariables.resolve(config.getKey()));
        endpoint =
            (!Utils.isEmpty(config.getEmulatorUrl()))
                ? newVariables.resolve(config.getEmulatorUrl())
                : String.format(Locale.ROOT, "https://%s.dfs.core.windows.net", account);
      }

      StorageSharedKeyCredential storageCreds = new StorageSharedKeyCredential(account, key);

      DataLakeServiceClient serviceClient =
          new DataLakeServiceClientBuilder()
              .endpoint(endpoint)
              .credential(storageCreds)
              // .httpClient((HttpClient) client)
              .buildClient();

      azureFileSystem =
          new AzureFileSystem(
              azureRootName,
              serviceClient,
              ((AzureFileName) fileName).getContainer(),
              fileSystemOptions,
              account);

    } finally {
      UserAuthenticatorUtils.cleanup(authData);
    }

    return azureFileSystem;
  }

  @Override
  public FileObject findFile(
      FileObject baseFileObject, String uri, FileSystemOptions fileSystemOptions)
      throws FileSystemException {
    return super.findFile(baseFileObject, uri, fileSystemOptions);
  }

  @Override
  protected FileObject findFile(FileName fileName, FileSystemOptions fileSystemOptions)
      throws FileSystemException {
    return super.findFile(fileName, fileSystemOptions);
  }

  @Override
  public Collection<Capability> getCapabilities() {
    return capabilities;
  }
}
