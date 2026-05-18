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

package org.apache.hop.git;

import lombok.Getter;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Image;

public class GitResource {

  @Getter private final Color ignoredColor;
  @Getter private final Color stagedUnchangedColor;
  @Getter private final Color stagedAddColor;
  @Getter private final Color stagedModifyColor;
  @Getter private final Color unstagedColor;

  @Getter private final Color stagedAddGrayColor;
  @Getter private final Color stagedModifyGrayColor;
  @Getter private final Color unstagedGrayColor;
  @Getter private final Color ignoredGrayColor;
  @Getter private final Color stagedUnchangedGrayColor;

  @Getter private final Color textInsertForegroundColor;
  @Getter private final Color textInsertBackgroundColor;
  @Getter private final Color textDeleteForegroundColor;
  @Getter private final Color textDeleteBackgroundColor;
  @Getter private final Color textChangeForegroundColor;
  @Getter private final Color textChangeBackgroundColor;

  @Getter private final Color textDifferenceColor;

  @Getter private final Image addImage;
  @Getter private final Image branchImage;
  @Getter private final Image cherryPickImage;
  @Getter private final Image diffImage;
  @Getter private final Image ignoreImage;
  @Getter private final Image localImage;
  @Getter private final Image lockImage;
  @Getter private final Image lockOpenImage;
  @Getter private final Image mergeImage;
  @Getter private final Image pullImage;
  @Getter private final Image pushImage;
  @Getter private final Image remoteImage;
  @Getter private final Image restoreImage;
  @Getter private final Image resetImage;
  @Getter private final Image tagImage;

  private static GitResource instance;

  /** Utility class */
  private GitResource() {
    GuiResource resource = GuiResource.getInstance();

    textInsertForegroundColor = resource.getColor(0, 255, 0);
    textInsertBackgroundColor = resource.getColor(41, 68, 54);
    // textInsertSashBackgroundColor = resource.getColor(31, 43, 38);
    textDeleteForegroundColor = resource.getColor(255, 0, 0);
    textDeleteBackgroundColor = resource.getColor(84, 36, 28);
    // textDeleteSashBackgroundColor = new Color(84, 36, 28);
    textChangeForegroundColor = resource.getColor(120, 191, 255);
    textChangeBackgroundColor = resource.getColor(56, 85, 112);
    //  textChangeSashBackgroundColor = new Color(37, 50, 62);
    textDifferenceColor = resource.getColor(153, 102, 0);

    // Adjust color for light/dark mode
    if (PropsUi.getInstance().isDarkMode()) {
      stagedModifyColor = resource.getColorLightBlue();
      ignoredColor = resource.getColorGray();
      unstagedColor = resource.getColor(217, 105, 73);
    } else {
      stagedModifyColor = resource.getColorBlue();
      ignoredColor = resource.getColorDarkGray();
      unstagedColor = resource.getColor(225, 30, 70);
    }
    stagedUnchangedColor = resource.getColorBlack();
    stagedAddColor = resource.getColorDarkGreen();

    // Muted variants when the explorer has already grayed the item (non-openable file).
    stagedAddGrayColor = resource.getColorDarkGreenMuted();
    stagedModifyGrayColor = resource.getColorLightBlueMuted();
    unstagedGrayColor = resource.getColorRedMuted();
    ignoredGrayColor = resource.getColorDarkGrayMuted();
    stagedUnchangedGrayColor = resource.getColorBlackMuted();

    addImage = getImage("git-add.svg");
    branchImage = getImage("branch.svg");
    cherryPickImage = getImage("cherry-pick.svg");
    diffImage = getImage("diff-text.svg");
    ignoreImage = getImage("ignore.svg");
    localImage = getImage("local.svg");
    lockImage = getImage("lock.svg");
    lockOpenImage = getImage("lock-open.svg");
    pullImage = getImage("pull.svg");
    pushImage = getImage("push.svg");
    mergeImage = getImage("git-merge.svg");
    remoteImage = getImage("remote.svg");
    restoreImage = getImage("git-restore.svg");
    resetImage = getImage("git-reset.svg");
    tagImage = getImage("tag.svg");
  }

  public static GitResource getInstance() {
    if (instance == null) {
      instance = new GitResource();
    }
    return instance;
  }

  public Image getImage(String location) {
    return GuiResource.getInstance()
        .getImage(
            location,
            getClass().getClassLoader(),
            ConstUi.SMALL_ICON_SIZE,
            ConstUi.SMALL_ICON_SIZE);
  }
}
