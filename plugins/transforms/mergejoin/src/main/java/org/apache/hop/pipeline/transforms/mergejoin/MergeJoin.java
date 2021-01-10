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

package org.apache.hop.pipeline.transforms.mergejoin;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.IStream;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Merge rows from 2 sorted streams and output joined rows with matched key fields. Use this instead
 * of hash join is both your input streams are too big to fit in memory. Note that both the inputs
 * must be sorted on the join key.
 *
 * <p>This is a first prototype implementation that only handles two streams and inner join. It also
 * always outputs all values from both streams. Ideally, we should: 1) Support any number of
 * incoming streams 2) Allow user to choose the join type (inner, outer) for each stream 3) Allow
 * user to choose which fields to push to next transform 4) Have multiple output ports as follows:
 * a) Containing matched records b) Unmatched records for each input port 5) Support incoming rows
 * to be sorted either on ascending or descending order. The currently implementation only supports
 * ascending
 */
public class MergeJoin extends BaseTransform<MergeJoinMeta, MergeJoinData>
    implements ITransform<MergeJoinMeta, MergeJoinData> {
  private static final Class<?> PKG = MergeJoinMeta.class; // For Translator

  public MergeJoin(
      TransformMeta transformMeta,
      MergeJoinMeta meta,
      MergeJoinData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  public boolean processRow() throws HopException {
    int compare;

    if (first) {
      first = false;

      // Find the RowSet to read from
      //
      List<IStream> infoStreams = meta.getTransformIOMeta().getInfoStreams();

      data.oneRowSet = findInputRowSet(infoStreams.get(0).getTransformName());
      if (data.oneRowSet == null) {
        throw new HopException(
            BaseMessages.getString(
                PKG,
                "MergeJoin.Exception.UnableToFindSpecifiedTransform",
                infoStreams.get(0).getTransformName()));
      }

      data.twoRowSet = findInputRowSet(infoStreams.get(1).getTransformName());
      if (data.twoRowSet == null) {
        throw new HopException(
            BaseMessages.getString(
                PKG,
                "MergeJoin.Exception.UnableToFindSpecifiedTransform",
                infoStreams.get(1).getTransformName()));
      }

      data.one = getRowFrom(data.oneRowSet);
      if (data.one != null) {
        data.oneMeta = data.oneRowSet.getRowMeta();
      } else {
        data.one = null;
        data.oneMeta =
            getPipelineMeta().getTransformFields(this, infoStreams.get(0).getTransformName());
      }

      data.two = getRowFrom(data.twoRowSet);
      if (data.two != null) {
        data.twoMeta = data.twoRowSet.getRowMeta();
      } else {
        data.two = null;
        data.twoMeta =
            getPipelineMeta().getTransformFields(this, infoStreams.get(1).getTransformName());
      }

      // just for speed: oneMeta+twoMeta
      //
      data.outputRowMeta = new RowMeta();
      data.outputRowMeta.mergeRowMeta(data.oneMeta.clone());
      data.outputRowMeta.mergeRowMeta(data.twoMeta.clone());

      if (data.one != null) {
        // Find the key indexes:
        data.keyNrs1 = new int[meta.getKeyFields1().length];
        for (int i = 0; i < data.keyNrs1.length; i++) {
          data.keyNrs1[i] = data.oneMeta.indexOfValue(meta.getKeyFields1()[i]);
          if (data.keyNrs1[i] < 0) {
            String message =
                BaseMessages.getString(
                    PKG,
                    "MergeJoin.Exception.UnableToFindFieldInReferenceStream",
                    meta.getKeyFields1()[i]);
            logError(message);
            throw new HopTransformException(message);
          }
        }
      }

      if (data.two != null) {
        // Find the key indexes:
        data.keyNrs2 = new int[meta.getKeyFields2().length];
        for (int i = 0; i < data.keyNrs2.length; i++) {
          data.keyNrs2[i] = data.twoMeta.indexOfValue(meta.getKeyFields2()[i]);
          if (data.keyNrs2[i] < 0) {
            String message =
                BaseMessages.getString(
                    PKG,
                    "MergeJoin.Exception.UnableToFindFieldInReferenceStream",
                    meta.getKeyFields2()[i]);
            logError(message);
            throw new HopTransformException(message);
          }
        }
      }

      // Calculate one_dummy... defaults to null
      data.one_dummy = RowDataUtil.allocateRowData(data.oneMeta.size() + data.twoMeta.size());

      // Calculate two_dummy... defaults to null
      //
      data.two_dummy = new Object[data.twoMeta.size()];
    }

    if (log.isRowLevel()) {
      logRowlevel(
          BaseMessages.getString(
                  PKG, "MergeJoin.Log.DataInfo", data.oneMeta.getString(data.one) + "")
              + data.twoMeta.getString(data.two));
    }

    /*
     * We can stop processing if any of the following is true: a) Both streams are empty b) First stream is empty and
     * join type is INNER or LEFT OUTER c) Second stream is empty and join type is INNER or RIGHT OUTER
     */
    if ((data.one == null && data.two == null)
        || (data.one == null && data.one_optional == false)
        || (data.two == null && data.two_optional == false)) {
      // Before we stop processing, we have to make sure that all rows from both input streams are
      // depleted!
      // If we don't do this, the pipeline can stall.
      //
      while (data.one != null && !isStopped()) {
        data.one = getRowFrom(data.oneRowSet);
      }
      while (data.two != null && !isStopped()) {
        data.two = getRowFrom(data.twoRowSet);
      }

      setOutputDone();
      return false;
    }

    if (data.one == null) {
      compare = -1;
    } else {
      if (data.two == null) {
        compare = 1;
      } else {
        int cmp =
            data.oneMeta.compare(data.one, data.twoMeta, data.two, data.keyNrs1, data.keyNrs2);
        compare = cmp > 0 ? 1 : cmp < 0 ? -1 : 0;
      }
    }

    switch (compare) {
      case 0:
        /*
         * We've got a match. This is what we do next (to handle duplicate keys correctly): Read the next record from
         * both streams If any of the keys match, this means we have duplicates. We therefore Create an array of all
         * rows that have the same keys Push a Cartesian product of the two arrays to output Else Just push the combined
         * rowset to output
         */
        data.one_next = getRowFrom(data.oneRowSet);
        data.two_next = getRowFrom(data.twoRowSet);

        int compare1 =
            (data.one_next == null)
                ? -1
                : data.oneMeta.compare(data.one, data.one_next, data.keyNrs1, data.keyNrs1);
        int compare2 =
            (data.two_next == null)
                ? -1
                : data.twoMeta.compare(data.two, data.two_next, data.keyNrs2, data.keyNrs2);
        if (compare1 == 0 || compare2 == 0) { // Duplicate keys

          if (data.ones == null) {
            data.ones = new ArrayList<>();
          } else {
            data.ones.clear();
          }
          if (data.twos == null) {
            data.twos = new ArrayList<>();
          } else {
            data.twos.clear();
          }
          data.ones.add(data.one);
          if (compare1 == 0) {
            // First stream has duplicates

            data.ones.add(data.one_next);
            for (; !isStopped(); ) {
              data.one_next = getRowFrom(data.oneRowSet);
              if (0
                  != ((data.one_next == null)
                      ? -1
                      : data.oneMeta.compare(
                          data.one, data.one_next, data.keyNrs1, data.keyNrs1))) {
                break;
              }
              data.ones.add(data.one_next);
            }
            if (isStopped()) {
              return false;
            }
          }
          data.twos.add(data.two);
          if (compare2 == 0) { // Second stream has duplicates

            data.twos.add(data.two_next);
            for (; !isStopped(); ) {
              data.two_next = getRowFrom(data.twoRowSet);
              if (0
                  != ((data.two_next == null)
                      ? -1
                      : data.twoMeta.compare(
                          data.two, data.two_next, data.keyNrs2, data.keyNrs2))) {
                break;
              }
              data.twos.add(data.two_next);
            }
            if (isStopped()) {
              return false;
            }
          }
          for (Iterator<Object[]> oneIter = data.ones.iterator();
              oneIter.hasNext() && !isStopped(); ) {
            Object[] one = oneIter.next();
            for (Iterator<Object[]> twoIter = data.twos.iterator();
                twoIter.hasNext() && !isStopped(); ) {
              Object[] two = twoIter.next();
              Object[] oneBig =
                  RowDataUtil.createResizedCopy(one, data.oneMeta.size() + data.twoMeta.size());
              Object[] combi = RowDataUtil.addRowData(oneBig, data.oneMeta.size(), two);
              putRow(data.outputRowMeta, combi);
            }
            // Remove the rows as we merge them to keep the overall memory footprint minimal
            oneIter.remove();
          }
          data.twos.clear();
        } else {
          // No duplicates

          Object[] outputRowData = RowDataUtil.addRowData(data.one, data.oneMeta.size(), data.two);
          putRow(data.outputRowMeta, outputRowData);
        }
        data.one = data.one_next;
        data.two = data.two_next;
        break;
      case 1:
        // if (log.isDebug()) logDebug("First stream has missing key");
        /*
         * First stream is greater than the second stream. This means: a) This key is missing in the first stream b)
         * Second stream may have finished So, if full/right outer join is set and 2nd stream is not null, we push a
         * record to output with only the values for the second row populated. Next, if 2nd stream is not finished, we
         * get a row from it; otherwise signal that we are done
         */
        if (data.one_optional == true) {
          if (data.two != null) {
            Object[] outputRowData =
                RowDataUtil.createResizedCopy(data.one_dummy, data.outputRowMeta.size());
            outputRowData = RowDataUtil.addRowData(outputRowData, data.oneMeta.size(), data.two);
            putRow(data.outputRowMeta, outputRowData);
            data.two = getRowFrom(data.twoRowSet);
          } else if (data.two_optional == false) {
            /*
             * If we are doing right outer join then we are done since there are no more rows in the second set
             */
            // Before we stop processing, we have to make sure that all rows from both input streams
            // are depleted!
            // If we don't do this, the pipeline can stall.
            //
            while (data.one != null && !isStopped()) {
              data.one = getRowFrom(data.oneRowSet);
            }
            while (data.two != null && !isStopped()) {
              data.two = getRowFrom(data.twoRowSet);
            }

            setOutputDone();
            return false;
          } else {
            /*
             * We are doing full outer join so print the 1st stream and get the next row from 1st stream
             */
            Object[] outputRowData =
                RowDataUtil.createResizedCopy(data.one, data.outputRowMeta.size());
            outputRowData =
                RowDataUtil.addRowData(outputRowData, data.oneMeta.size(), data.two_dummy);
            putRow(data.outputRowMeta, outputRowData);
            data.one = getRowFrom(data.oneRowSet);
          }
        } else if (data.two == null && data.two_optional == true) {
          /**
           * We have reached the end of stream 2 and there are records present in the first stream.
           * Also, join is left or full outer. So, create a row with just the values in the first
           * stream and push it forward
           */
          Object[] outputRowData =
              RowDataUtil.createResizedCopy(data.one, data.outputRowMeta.size());
          outputRowData =
              RowDataUtil.addRowData(outputRowData, data.oneMeta.size(), data.two_dummy);
          putRow(data.outputRowMeta, outputRowData);
          data.one = getRowFrom(data.oneRowSet);
        } else if (data.two != null) {
          /*
           * We are doing an inner or left outer join, so throw this row away from the 2nd stream
           */
          data.two = getRowFrom(data.twoRowSet);
        }
        break;
      case -1:
        // if (log.isDebug()) logDebug("Second stream has missing key");
        /*
         * Second stream is greater than the first stream. This means: a) This key is missing in the second stream b)
         * First stream may have finished So, if full/left outer join is set and 1st stream is not null, we push a
         * record to output with only the values for the first row populated. Next, if 1st stream is not finished, we
         * get a row from it; otherwise signal that we are done
         */
        if (data.two_optional == true) {
          if (data.one != null) {
            Object[] outputRowData =
                RowDataUtil.createResizedCopy(data.one, data.outputRowMeta.size());
            outputRowData =
                RowDataUtil.addRowData(outputRowData, data.oneMeta.size(), data.two_dummy);
            putRow(data.outputRowMeta, outputRowData);
            data.one = getRowFrom(data.oneRowSet);
          } else if (data.one_optional == false) {
            /*
             * We are doing a left outer join and there are no more rows in the first stream; so we are done
             */
            // Before we stop processing, we have to make sure that all rows from both input streams
            // are depleted!
            // If we don't do this, the pipeline can stall.
            //
            while (data.one != null && !isStopped()) {
              data.one = getRowFrom(data.oneRowSet);
            }
            while (data.two != null && !isStopped()) {
              data.two = getRowFrom(data.twoRowSet);
            }

            setOutputDone();
            return false;
          } else {
            /*
             * We are doing a full outer join so print the 2nd stream and get the next row from the 2nd stream
             */
            Object[] outputRowData =
                RowDataUtil.createResizedCopy(data.one_dummy, data.outputRowMeta.size());
            outputRowData = RowDataUtil.addRowData(outputRowData, data.oneMeta.size(), data.two);
            putRow(data.outputRowMeta, outputRowData);
            data.two = getRowFrom(data.twoRowSet);
          }
        } else if (data.one == null && data.one_optional == true) {
          /*
           * We have reached the end of stream 1 and there are records present in the second stream. Also, join is right
           * or full outer. So, create a row with just the values in the 2nd stream and push it forward
           */
          Object[] outputRowData =
              RowDataUtil.createResizedCopy(data.one_dummy, data.outputRowMeta.size());
          outputRowData = RowDataUtil.addRowData(outputRowData, data.oneMeta.size(), data.two);
          putRow(data.outputRowMeta, outputRowData);
          data.two = getRowFrom(data.twoRowSet);
        } else if (data.one != null) {
          /*
           * We are doing an inner or right outer join so a non-matching row in the first stream is of no use to us -
           * throw it away and get the next row
           */
          data.one = getRowFrom(data.oneRowSet);
        }
        break;
      default:
        logDebug("We shouldn't be here!!");
        // Make sure we do not go into an infinite loop by continuing to read data
        data.one = getRowFrom(data.oneRowSet);
        data.two = getRowFrom(data.twoRowSet);
        break;
    }
    if (checkFeedback(getLinesRead())) {
      logBasic(BaseMessages.getString(PKG, "MergeJoin.LineNumber") + getLinesRead());
    }
    return true;
  }

  public boolean init() {

    if (super.init()) {
      List<IStream> infoStreams = meta.getTransformIOMeta().getInfoStreams();
      if (infoStreams.get(0).getTransformMeta() == null
          || infoStreams.get(1).getTransformMeta() == null) {
        logError(BaseMessages.getString(PKG, "MergeJoin.Log.BothTrueAndFalseNeeded"));
        return false;
      }
      String joinType = meta.getJoinType();
      for (int i = 0; i < MergeJoinMeta.joinTypes.length; ++i) {
        if (joinType.equalsIgnoreCase(MergeJoinMeta.joinTypes[i])) {
          data.one_optional = MergeJoinMeta.one_optionals[i];
          data.two_optional = MergeJoinMeta.two_optionals[i];
          return true;
        }
      }
      logError(BaseMessages.getString(PKG, "MergeJoin.Log.InvalidJoinType", meta.getJoinType()));
      return false;
    }
    return true;
  }

  /**
   * Checks whether incoming rows are join compatible. This essentially means that the keys being
   * compared should be of the same datatype and both rows should have the same number of keys
   * specified
   *
   * @param row1 Reference row
   * @param row2 Row to compare to
   * @return true when templates are compatible.
   */
  protected boolean isInputLayoutValid(IRowMeta row1, IRowMeta row2) {
    if (row1 != null && row2 != null) {
      // Compare the key types
      String[] keyFields1 = meta.getKeyFields1();
      int nrKeyFields1 = keyFields1.length;
      String[] keyFields2 = meta.getKeyFields2();
      int nrKeyFields2 = keyFields2.length;

      if (nrKeyFields1 != nrKeyFields2) {
        logError("Number of keys do not match " + nrKeyFields1 + " vs " + nrKeyFields2);
        return false;
      }

      for (int i = 0; i < nrKeyFields1; i++) {
        IValueMeta v1 = row1.searchValueMeta(keyFields1[i]);
        if (v1 == null) {
          return false;
        }
        IValueMeta v2 = row2.searchValueMeta(keyFields2[i]);
        if (v2 == null) {
          return false;
        }
        if (v1.getType() != v2.getType()) {
          return false;
        }
      }
    }
    // we got here, all seems to be ok.
    return true;
  }
}
