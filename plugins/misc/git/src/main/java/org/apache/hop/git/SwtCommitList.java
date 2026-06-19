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
import org.apache.hop.git.util.DistinctColor;
import org.apache.hop.ui.core.gui.GuiResource;
import org.eclipse.jgit.revplot.PlotCommitList;
import org.eclipse.jgit.revplot.PlotLane;
import org.eclipse.swt.graphics.Color;

public class SwtCommitList extends PlotCommitList<SwtCommitList.Lane> {
  private Color lastColor;

  SwtCommitList() {
    lastColor = GuiResource.getInstance().getColorGreen();
  }

  @Override
  protected Lane createLane() {
    lastColor = DistinctColor.nextColor(lastColor);
    return new Lane(lastColor);
  }

  @Override
  protected void recycleLane(Lane lane) {
    lane.color = DistinctColor.nextColor(lane.getColor());
    lastColor = lane.getColor();
  }

  public static class Lane extends PlotLane {
    @Getter private Color color;

    public Lane(Color color) {
      this.color = color;
    }
  }
}
