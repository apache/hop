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

package org.apache.hop.ui.hopgui;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.service.UISession;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * A dialog that looks up its input fields on a thread of its own reads session scoped GUI state
 * while doing so - {@code PropsUi} through {@code ConstUi.sortFieldNames}. In Hop Web that state
 * belongs to a RAP session, and a thread with no session gets "Invalid thread access" instead.
 */
class BackgroundThreadFacadeImplTest {

  @Test
  @DisplayName("work started from a session runs with that session bound")
  void carriesTheSessionOverToTheBackgroundThread() {
    UISession uiSession = mock(UISession.class);
    when(uiSession.isBound()).thenReturn(true);
    Runnable work = mock(Runnable.class);

    try (MockedStatic<RWT> rwt = mockStatic(RWT.class)) {
      rwt.when(() -> RWT.getUISession()).thenReturn(uiSession);

      Runnable bound = new BackgroundThreadFacadeImpl().bindInternal(work);

      // The session is read here, on the thread that starts the work; the work itself waits.
      verifyNoInteractions(work);

      bound.run();

      // exec() makes the session current for whichever thread runs the work, which is the point.
      verify(uiSession).exec(work);
    }
  }

  /**
   * A session that ended while the work was queued must not be made current: RWT hands out a
   * context that resolves to no session, and reading it back fails with a NullPointerException as
   * far away as {@code Display.getCurrent()} (issue #8248). The work still runs.
   */
  @Test
  @DisplayName("a session that ended in the meantime is not carried over")
  void runsWithoutASessionThatDiedBeforeTheWorkStarted() {
    UISession uiSession = mock(UISession.class);
    when(uiSession.isBound()).thenReturn(false);
    Runnable work = mock(Runnable.class);

    try (MockedStatic<RWT> rwt = mockStatic(RWT.class)) {
      rwt.when(() -> RWT.getUISession()).thenReturn(uiSession);

      new BackgroundThreadFacadeImpl().bindInternal(work).run();

      verify(work).run();
      verify(uiSession, never()).exec(work);
    }
  }

  @Test
  @DisplayName("without a session there is nothing to carry over")
  void leavesTheWorkAloneOutsideASession() {
    Runnable work = mock(Runnable.class);

    try (MockedStatic<RWT> rwt = mockStatic(RWT.class)) {
      rwt.when(() -> RWT.getUISession())
          .thenThrow(new IllegalStateException("Invalid thread access"));

      assertSame(work, new BackgroundThreadFacadeImpl().bindInternal(work));
    }
  }
}
