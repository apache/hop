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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.util.Assert;

final class DynamicWaitTimes {

  static SingleStreamStatus build(
      List<IRowSet> rowSets, Supplier<Integer> supplier, Integer waitTime) {
    if (rowSets.size() == 1) {
      return new SingleStreamStatus(waitTime);
    }
    return new MultiStreamStatus(new ArrayList<>(rowSets), supplier, waitTime);
  }

  static class SingleStreamStatus {
    protected boolean active = true;
    private long interval = 1;
    private long waitTime;

    SingleStreamStatus(Integer waitTime) {
      this.waitTime = waitTime;
    }

    public long get() {
      return interval;
    }

    public void reset() {
      interval = 1;
      active = true;
    }

    public void adjust(boolean timeout, IRowSet nextIfExist) {
      if (allowAdjust() && timeout) {
        if (interval == waitTime) {
          active = false;
        }
        interval = interval * 2;
        if (interval > waitTime) {
          interval = waitTime;
        }
      }
    }

    public void remove(IRowSet rowSet) {}

    protected boolean allowAdjust() {
      return active;
    }

    /** only for test */
    protected void doReset(int index) {}
  }

  private static class MultiStreamStatus extends SingleStreamStatus {
    private final List<IRowSet> streamList;
    private final List<SingleStreamStatus> statusList;
    private final Supplier<Integer> supplier;

    MultiStreamStatus(List<IRowSet> rowSets, Supplier<Integer> supplier, Integer waitTime) {
      super(waitTime);
      this.streamList = rowSets;
      this.supplier = supplier;
      this.statusList = new ArrayList<>(rowSets.size());
      for (int i = 0; i < rowSets.size(); i++) {
        statusList.add(new SingleStreamStatus(waitTime));
      }
    }

    @Override
    public long get() {
      SingleStreamStatus stream = statusList.get(supplier.get());
      return stream.active ? stream.get() : 0;
    }

    @Override
    public void reset() {
      statusList.get(supplier.get()).reset();
    }

    @Override
    public void adjust(boolean timeout, IRowSet nextIfExist) {
      final int index = supplier.get();
      if (index == -1) {
        return;
      }
      SingleStreamStatus current = statusList.get(index);
      if (streamList.get(index).equals(nextIfExist)) {
        if (streamList.size() == 1 && !current.active) {
          current.reset();
        }
      } else {
        // reset delay time when switch next stream
        int nextIndex = streamList.indexOf(nextIfExist);
        Assert.assertTrue(nextIndex == 0 || nextIndex == index + 1);
        current = statusList.get(nextIndex);
        if (activeIfNeed()) {
          current.reset();
          return;
        }
      }

      current.adjust(timeout, nextIfExist);
    }

    @Override
    public void remove(IRowSet rowSet) {
      int index = supplier.get();
      if (!streamList.get(index).equals(rowSet)) {
        // get index for some merge flow transformations
        index = streamList.indexOf(rowSet);
      }
      Assert.assertTrue(index > -1, "Removed input steam at {0}, before switch next stream", index);
      streamList.remove(index);
      statusList.remove(index);
      if (!streamList.isEmpty()) {
        // reactive all stream
        if (activeIfNeed()) {
          statusList.forEach(SingleStreamStatus::reset);
        }
      }
    }

    @Override
    protected boolean allowAdjust() {
      return activeIfNeed();
    }

    private boolean activeIfNeed() {
      return statusList.stream().noneMatch(SingleStreamStatus::allowAdjust);
    }

    @Override
    protected void doReset(int index) {
      statusList.get(index).reset();
    }
  }
}
