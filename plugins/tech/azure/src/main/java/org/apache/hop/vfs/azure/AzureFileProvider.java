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

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.UserAuthenticationData;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;
import org.apache.commons.vfs2.provider.GenericFileName;
import org.apache.commons.vfs2.util.UserAuthenticatorUtils;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import org.apache.hop.vfs.azure.config.AzureConfig;
import org.apache.hop.vfs.azure.config.AzureConfigSingleton;

public class AzureFileProvider extends AbstractOriginatingFileProvider {

	public final static Collection<Capability> capabilities = Collections
			.unmodifiableCollection(Arrays.asList(Capability.CREATE, Capability.DELETE,
					// Capability.RENAME,
					Capability.ATTRIBUTES, Capability.GET_TYPE, Capability.GET_LAST_MODIFIED, Capability.LIST_CHILDREN,
					Capability.READ_CONTENT, Capability.URI, Capability.WRITE_CONTENT));

	public final static UserAuthenticationData.Type[] AUTHENTICATOR_TYPES = new UserAuthenticationData.Type[] {
			UserAuthenticationData.USERNAME, UserAuthenticationData.PASSWORD };

	private static FileSystemOptions defaultOptions = new FileSystemOptions();

	public static FileSystemOptions getDefaultFileSystemOptions() {
		return defaultOptions;
	}

	private Log logger = LogFactory.getLog(AzureFileProvider.class);

	public AzureFileProvider() {
		super();
		setFileNameParser(AzureFileNameParser.getInstance());
	}

	protected FileSystem doCreateFileSystem(FileName fileName, FileSystemOptions fileSystemOptions)
			throws FileSystemException {

		FileSystemOptions fsOptions = fileSystemOptions != null ? fileSystemOptions : getDefaultFileSystemOptions();
		UserAuthenticationData authData = null;
		CloudBlobClient service;
		try {
			authData = UserAuthenticatorUtils.authenticate(fsOptions, AUTHENTICATOR_TYPES);

			logger.info("Initialize Azure client");

			AzureConfig config = AzureConfigSingleton.getConfig();

			if ( StringUtils.isEmpty(config.getAccount())) {
				throw new FileSystemException( "Please configure the Azure account to use in the configuration (Options dialog or with hop-conf)" );
			}
			if ( StringUtils.isEmpty(config.getKey())) {
				throw new FileSystemException( "Please configure the Azure key to use in the configuration (Options dialog or with hop-conf)" );
			}

			String account = config.getAccount();

/*
			if (account == null || account.length() == 0)
				account = ((GenericFileName) fileName).getUserName();
*/

			String key = config.getKey();

/*
			if (key == null || key.length() == 0)
				key = ((GenericFileName) fileName).getPassword();
*/

			String storageConnectionString = String.format("DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net",
					account, key);
			CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);

			service = storageAccount.createCloudBlobClient();

		} catch (InvalidKeyException e) {
			throw new FileSystemException(e.getMessage(), e);
		} catch (URISyntaxException e) {
			throw new FileSystemException(e.getMessage(), e);
		} finally {
			UserAuthenticatorUtils.cleanup(authData);
		}

		return new AzureFileSystem((AzureFileName) fileName, service, fsOptions);
	}

	public Collection<Capability> getCapabilities() {
		return capabilities;
	}
}