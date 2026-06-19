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
 */

package org.apache.hop.ui.pipeline.transform.common;

import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.InputStream;
import java.io.InputStreamReader;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transforms.common.ICsvInputAwareMeta;
import org.junit.jupiter.api.Test;

class ICsvInputAwareTransformDialogTest {

  /**
   * Minimal implementation that simulates a file which cannot be opened (e.g. it does not exist),
   * in which case {@link #getInputStream(ICsvInputAwareMeta)} returns null.
   */
  private static class NoFileDialog implements ICsvInputAwareTransformDialog {
    @Override
    public InputStream getInputStream(final ICsvInputAwareMeta meta) {
      return null; // file does not exist / cannot be read
    }

    @Override
    public ICsvInputAwareImportProgressDialog getCsvImportProgressDialog(
        final ICsvInputAwareMeta meta, final int samples, final InputStreamReader reader) {
      throw new AssertionError("should not be reached when there is no input stream");
    }

    @Override
    public LogChannel getLogChannel() {
      return null;
    }

    @Override
    public PipelineMeta getPipelineMeta() {
      return null;
    }

    @Override
    public IVariables getVariables() {
      return new Variables();
    }
  }

  /** A null input stream (missing file) must yield a null reader, not a NullPointerException. */
  @Test
  void getReaderReturnsNullForNullInputStream() {
    final NoFileDialog dlg = new NoFileDialog();
    assertNull(dlg.getReader(null, null));
  }

  /**
   * When the file cannot be opened, loading fields must return cleanly (null) instead of throwing a
   * NullPointerException while building the reader or closing the (null) input stream.
   */
  @Test
  void loadFieldsImplReturnsNullWhenFileCannotBeOpened() {
    final NoFileDialog dlg = new NoFileDialog();
    assertNull(dlg.loadFieldsImpl((ICsvInputAwareMeta) null, 100));
  }
}
