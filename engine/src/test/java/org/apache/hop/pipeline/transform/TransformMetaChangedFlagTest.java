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

package org.apache.hop.pipeline.transform;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.gui.Point;
import org.apache.hop.pipeline.transform.transforms.FakeMeta;
import org.junit.jupiter.api.Test;

/**
 * Transform dialogs are not modal, so the canvas stays live while one is open. Every dialog
 * captures {@code changed = input.hasChanged()} when it is constructed and restores it in cancel(),
 * which used to wipe anything the canvas had done to the same flag in the meantime. Issue #8022.
 *
 * <p>The wrapper now keeps its own flag for the state it owns, so a dialog's Cancel can only roll
 * back the transform's own settings.
 */
class TransformMetaChangedFlagTest {

  private static TransformMeta cleanTransform() {
    TransformMeta transformMeta = new TransformMeta("T1", new FakeMeta());
    transformMeta.setLocation(new Point(100, 100));
    transformMeta.setChanged(false);
    return transformMeta;
  }

  @Test
  void cancelKeepsACanvasMoveMadeWhileTheDialogWasOpen() {
    TransformMeta transformMeta = cleanTransform();
    ITransformMeta input = transformMeta.getTransform();

    // The dialog is constructed and captures the flag as it finds it.
    boolean changed = input.hasChanged();

    // The user drags the transform on the canvas while the dialog is still open.
    transformMeta.setLocation(new Point(400, 250));
    assertTrue(transformMeta.hasChanged(), "moving a transform marks it changed");

    // The user goes back to the dialog and presses Cancel.
    input.setChanged(changed);

    assertTrue(transformMeta.hasChanged(), "Cancel must not discard the move");
  }

  @Test
  void cancelStillRollsBackTheDialogsOwnEditing() {
    TransformMeta transformMeta = cleanTransform();
    ITransformMeta input = transformMeta.getTransform();

    boolean changed = input.hasChanged();

    // Typing in a dialog field marks the transform's own settings as changed.
    input.setChanged();
    assertTrue(transformMeta.hasChanged());

    input.setChanged(changed);

    assertFalse(transformMeta.hasChanged(), "Cancel rolls back the dialog's own marking");
  }

  @Test
  void otherWrapperLevelEditsAlsoSurviveCancel() {
    TransformMeta transformMeta = cleanTransform();
    ITransformMeta input = transformMeta.getTransform();

    boolean changed = input.hasChanged();

    // Right-click actions on the canvas change settings the wrapper owns.
    transformMeta.setCopies(4);
    transformMeta.setDistributes(false);

    input.setChanged(changed);

    assertTrue(transformMeta.hasChanged(), "copies and distribution are wrapper level state");
  }

  @Test
  void clearChangedResetsBothFlags() {
    TransformMeta transformMeta = cleanTransform();
    transformMeta.setLocation(new Point(400, 250));
    transformMeta.getTransform().setChanged();
    assertTrue(transformMeta.hasChanged());

    // What PipelineMeta.clearChanged() does after a save.
    transformMeta.setChanged(false);

    assertFalse(transformMeta.hasChanged(), "saving clears the wrapper flag as well");
  }
}
