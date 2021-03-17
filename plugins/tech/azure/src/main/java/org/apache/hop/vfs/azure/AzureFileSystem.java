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

import java.util.Collection;

import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;

import com.microsoft.azure.storage.blob.CloudBlobClient;

public class AzureFileSystem extends AbstractFileSystem {

	private CloudBlobClient client;

	public AzureFileSystem(AzureFileName fileName, CloudBlobClient service, FileSystemOptions fileSystemOptions)
			throws FileSystemException {
		super(fileName, null, fileSystemOptions);
		this.client = service;
	}

	@Override
	protected void addCapabilities(Collection<Capability> capabilities) {
		capabilities.addAll(AzureFileProvider.capabilities);
	}

	@Override
	protected FileObject createFile(AbstractFileName name) throws Exception {
		return new AzureFileObject(name, this, client);
	}
}