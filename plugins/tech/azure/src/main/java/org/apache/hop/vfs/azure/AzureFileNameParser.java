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

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.HostFileNameParser;
import org.apache.commons.vfs2.provider.UriParser;
import org.apache.commons.vfs2.provider.VfsComponentContext;

public class AzureFileNameParser extends HostFileNameParser {

	private static final AzureFileNameParser instance = new AzureFileNameParser();

	public static AzureFileNameParser getInstance() {
		return instance;
	}

	public AzureFileNameParser() {
		super(443);
	}

	@Override
	public FileName parseUri(final VfsComponentContext context, FileName base, String filename)
		throws FileSystemException {
		final StringBuilder name = new StringBuilder();
		Authority auth = null;
		String path = null;
		FileType fileType;

		int eidx = filename.indexOf("@/");
		if (eidx != -1)
			filename = filename.substring(0,  eidx + 1) + "windowsazure.com" + filename.substring(eidx + 1);

		String scheme;
		try {
			auth = extractToPath(filename, name);
			if (auth.getUserName() == null) {
				scheme = UriParser.extractScheme(filename, name);
				UriParser.canonicalizePath(name, 0, name.length(), this);
				UriParser.fixSeparators(name);
			} else {
				scheme = auth.getScheme();
			}
			fileType = UriParser.normalisePath(name);
			path = name.toString();
			if (path.equals("")) {
				path = "/";
			}
		} catch (FileSystemException fse) {
			scheme = UriParser.extractScheme(filename, name);
			UriParser.canonicalizePath(name, 0, name.length(), this);
			UriParser.fixSeparators(name);
			// final String rootFile = extractRootPrefix(filename, name);
			fileType = UriParser.normalisePath(name);
			path = name.toString();

		}
		return new AzureFileName(scheme, path, fileType);
	}

}