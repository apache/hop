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

package org.apache.hop.ui.i18n;

import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.vfs.HopVfs;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class MessagesSourceCrawlerTest {

  private MessagesSourceCrawler crawler;

  @Before
  public void before() throws Exception {
    HopEnvironment.init();
    crawler = new MessagesSourceCrawler( LogChannel.GENERAL, ".", new BundlesStore() );
  }

  @Test
  public void lookForOccurrencesInFile_HopGui() throws Exception {
    String sourceFolder = "src/main/java/org/apache/hop/ui/hopgui/";
    String sourceFile = sourceFolder+"HopGui.java";
    FileObject fileObject = HopVfs.getFileObject( sourceFile );

    crawler.lookForOccurrencesInFile( sourceFolder, fileObject);

    List<KeyOccurrence> keyOccurrences = crawler.getKeyOccurrences( sourceFolder );
    assertFalse(keyOccurrences.isEmpty());
  }

  @Test
  public void lookForOccurrencesInFile_PkgReferenceByOtherClass() throws Exception {

    String sourceFolder = "../plugins/actions/mailvalidator/src/main/java/org/apache/hop/workflow/actions/mailvalidator/";
    String sourceFile = sourceFolder+"MailValidation.java";
    FileObject fileObject = HopVfs.getFileObject( sourceFile );

    crawler.lookForOccurrencesInFile( sourceFolder, fileObject );

    List<KeyOccurrence> keyOccurrences = crawler.getKeyOccurrences( sourceFolder );
    assertFalse(keyOccurrences.isEmpty());

  }
}