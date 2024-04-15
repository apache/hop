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

package org.apache.hop.vfs.azure;

import java.util.Arrays;
import java.util.Collection;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.VfsComponentContext;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
@RunWith(Parameterized.class)
public class AzureFileNameParserTest {

    private AzureFileNameParser parser;

    private final String inputUri;
    private final String expectedScheme;
    private final String expectedContainer;
    private final String expectedPathAfterContainer;

    private final FileType expectedType;

    @Before
    public void setup(){
        parser = new AzureFileNameParser();
    }

    public AzureFileNameParserTest(String inputUri, String expectedScheme, String expectedContainer, String expectedPathAfterContainer, FileType expectedType) {
        this.inputUri = inputUri;
        this.expectedScheme = expectedScheme;
        this.expectedContainer = expectedContainer;
        this.expectedPathAfterContainer = expectedPathAfterContainer;
        this.expectedType = expectedType;
    }

    @Parameterized.Parameters
    public static Collection azureUris() {
        return Arrays.asList(new Object[][] {
            { "azfs://hopsa/container/folder1/parquet-test-delo2-azfs-00-0001.parquet","azfs", "container","/folder1/parquet-test-delo2-azfs-00-0001.parquet", FileType.FILE },
            { "azfs:/hopsa/container/folder1/", "azfs", "container","/folder1", FileType.FOLDER },
            { "azure://test/folder1/", "azure", "test", "/folder1", FileType.FOLDER },
            { "azure://mycontainer/folder1/parquet-test-delo2-azfs-00-0001.parquet","azure", "mycontainer","/folder1/parquet-test-delo2-azfs-00-0001.parquet", FileType.FILE },
            { "azfs://devstoreaccount1/delo/delo3-azfs-00-0001.parquet", "azfs", "delo", "/delo3-azfs-00-0001.parquet", FileType.FILE}
        });
    }

    @Test
    public void parseUri() throws FileSystemException {
        VfsComponentContext context = Mockito.mock(VfsComponentContext.class);

        AzureFileName actual = (AzureFileName) parser.parseUri(context, null, inputUri);

        assertEquals(expectedScheme, actual.getScheme());
        assertEquals(expectedContainer, actual.getContainer());
        assertEquals(expectedPathAfterContainer, actual.getPathAfterContainer());
        assertEquals(expectedType, actual.getType());
    }
}
