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

package org.apache.hop.workflow.action;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.gui.Point;
import org.apache.hop.workflow.actions.ActionFake;
import org.junit.jupiter.api.Test;

/**
 * The action side of issue #8022: action dialogs are not modal either, and they use the same
 * "capture the flag on open, restore it on cancel" idiom as transform dialogs.
 */
class ActionMetaChangedFlagTest {

  private static ActionMeta cleanAction() {
    ActionMeta actionMeta = new ActionMeta(new ActionFake());
    actionMeta.setLocation(new Point(100, 100));
    actionMeta.clearChanged();
    return actionMeta;
  }

  @Test
  void cancelKeepsACanvasMoveMadeWhileTheDialogWasOpen() {
    ActionMeta actionMeta = cleanAction();
    IAction action = actionMeta.getAction();

    boolean changed = action.hasChanged();

    actionMeta.setLocation(new Point(400, 250));
    assertTrue(actionMeta.hasChanged(), "moving an action marks it changed");

    action.setChanged(changed);

    assertTrue(actionMeta.hasChanged(), "Cancel must not discard the move");
  }

  @Test
  void cancelStillRollsBackTheDialogsOwnEditing() {
    ActionMeta actionMeta = cleanAction();
    IAction action = actionMeta.getAction();

    boolean changed = action.hasChanged();

    action.setChanged();
    assertTrue(actionMeta.hasChanged());

    action.setChanged(changed);

    assertFalse(actionMeta.hasChanged(), "Cancel rolls back the dialog's own marking");
  }

  @Test
  void clearChangedResetsBothFlags() {
    ActionMeta actionMeta = cleanAction();
    actionMeta.setLocation(new Point(400, 250));
    actionMeta.getAction().setChanged();
    assertTrue(actionMeta.hasChanged());

    actionMeta.clearChanged();

    assertFalse(actionMeta.hasChanged(), "saving clears the wrapper flag as well");
  }
}
