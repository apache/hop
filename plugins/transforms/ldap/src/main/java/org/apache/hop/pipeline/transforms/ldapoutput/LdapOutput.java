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
package org.apache.hop.pipeline.transforms.ldapoutput;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.ldapinput.LdapConnection;

/**
 * Write to LDAP.
 *
 * @author Samatar
 * @since 21-09-2007
 */
public class LdapOutput extends BaseTransform<LdapOutputMeta, LdapOutputData>
    implements ITransform<LdapOutputMeta, LdapOutputData> {
  private static Class<?> classFromResourcesPackage =
      LdapOutputMeta.class; // For Translator

  public LdapOutput(
      TransformMeta transformMeta,
      LdapOutputMeta meta,
      LdapOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  public boolean processRow() throws HopException {

    // Grab a row and also wait for a row to be finished.
    Object[] outputRowData = getRow();

    if (outputRowData == null) {
      setOutputDone();
      return false;
    }
    if (first) {
      first = false;

      if (meta.getOperationType() != LdapOutputMeta.OPERATION_TYPE_DELETE
          && meta.getOperationType() != LdapOutputMeta.OPERATION_TYPE_RENAME) {

        // get total fields in the grid
        data.nrFields = meta.getUpdateLookup().length;

        // Check if field list is filled
        if (data.nrFields == 0) {
          throw new HopException(
              BaseMessages.getString(
                  classFromResourcesPackage, "LdapOutputUpdateDialog.FieldsMissing.DialogMessage"));
        }

        // Take care of variable
        data.fieldsAttribute = new String[data.nrFields];
        // Build the mapping of input position to field name
        data.fieldStream = new int[data.nrFields];

        // Fields to update
        List<Integer> fieldsToUpdateInStreaml = new ArrayList<>();
        List<String> fieldsToUpdateAttributel = new ArrayList<>();

        for (int i = 0; i < data.nrFields; i++) {

          data.fieldStream[i] =
              getInputRowMeta().indexOfValue( resolve(meta.getUpdateStream()[i]));
          if (data.fieldStream[i] < 0) {
            throw new HopException(
                "Field [" + meta.getUpdateStream()[i] + "] couldn't be found in the input stream!");
          }
          data.fieldsAttribute[i] = resolve(meta.getUpdateLookup()[i]);

          if (meta.getOperationType() == LdapOutputMeta.OPERATION_TYPE_UPSERT
              && meta.getUpdate()[i].booleanValue()) {
            // We need also to keep care of the fields to update
            fieldsToUpdateInStreaml.add(data.fieldStream[i]);
            fieldsToUpdateAttributel.add(data.fieldsAttribute[i]);
          }
        }

        data.nrFieldsToUpdate = fieldsToUpdateInStreaml.size();
        if (data.nrFieldsToUpdate > 0) {
          data.fieldStreamToUpdate = new int[data.nrFieldsToUpdate];
          data.fieldsAttributeToUpdate = new String[data.nrFieldsToUpdate];
          for (int i = 0; i < fieldsToUpdateInStreaml.size(); i++) {
            data.fieldStreamToUpdate[i] = fieldsToUpdateInStreaml.get(i);
            data.fieldsAttributeToUpdate[i] = fieldsToUpdateAttributel.get(i);
          }
        }

        data.attributes = new String[data.nrFields];
        if (meta.getOperationType() == LdapOutputMeta.OPERATION_TYPE_UPSERT
            && data.nrFieldsToUpdate > 0) {
          data.attributesToUpdate = new String[data.nrFieldsToUpdate];
        }
      }

      if (meta.getOperationType() == LdapOutputMeta.OPERATION_TYPE_RENAME) {
        String oldDnField = resolve(meta.getOldDnFieldName());
        if (Utils.isEmpty(oldDnField)) {
          throw new HopException(
              BaseMessages.getString(
                  classFromResourcesPackage, "LdapOutput.Error.OldDNFieldMissing"));
        }

        String newDnField = resolve(meta.getNewDnFieldName());
        if (Utils.isEmpty(newDnField)) {
          throw new HopException(
              BaseMessages.getString(
                  classFromResourcesPackage, "LdapOutput.Error.NewDNFieldMissing"));
        }

        // return the index of the field in the input stream
        data.indexOfOldDNField = getInputRowMeta().indexOfValue(oldDnField);

        if (data.indexOfOldDNField < 0) {
          // the field is unreachable!
          throw new HopException(
              BaseMessages.getString(
                  classFromResourcesPackage, "LdapOutput.Error.CanNotFindField", oldDnField));
        }
        // return the index of the field in the input stream
        data.indexOfNewDNField = getInputRowMeta().indexOfValue(newDnField);

        if (data.indexOfNewDNField < 0) {
          // the field is unreachable!
          throw new HopException(
              BaseMessages.getString(
                  classFromResourcesPackage, "LdapOutput.Error.CanNotFindField", newDnField));
        }

      } else {
        String dnField = resolve(meta.getDnField());
        // Check Dn field
        if (Utils.isEmpty(dnField)) {
          throw new HopException(
              BaseMessages.getString(classFromResourcesPackage, "LdapOutput.Error.DNFieldMissing"));
        }

        // return the index of the field in the input stream
        data.indexOfDNField = getInputRowMeta().indexOfValue(dnField);

        if (data.indexOfDNField < 0) {
          // the field is unreachable!
          throw new HopException(
              BaseMessages.getString(
                  classFromResourcesPackage, "LdapOutput.Error.CanNotFindField", dnField));
        }
      }
    }

    incrementLinesInput();
    String dn = null;

    try {
      if (meta.getOperationType() != LdapOutputMeta.OPERATION_TYPE_RENAME) {
        // Get DN
        dn = getInputRowMeta().getString(outputRowData, data.indexOfDNField);

        if (isDebug()) {
          logDebug(BaseMessages.getString(classFromResourcesPackage, "LdapOutput.ProcessDn", dn));
        }

        if (meta.getOperationType() != LdapOutputMeta.OPERATION_TYPE_DELETE) {
          // Build new value attributes
          for (int i = 0; i < data.nrFields; i++) {
            data.attributes[i] = getInputRowMeta().getString(outputRowData, data.fieldStream[i]);
          }
        }
      }
      switch (meta.getOperationType()) {
        case LdapOutputMeta.OPERATION_TYPE_UPSERT:
          // handle fields to update
          for (int i = 0; i < data.nrFieldsToUpdate; i++) {
            data.attributesToUpdate[i] =
                getInputRowMeta().getString(outputRowData, data.fieldStreamToUpdate[i]);
          }
          int status =
              data.connection.upsert(
                  dn,
                  data.fieldsAttribute,
                  data.attributes,
                  data.fieldsAttributeToUpdate,
                  data.attributesToUpdate,
                  data.separator);
          switch (status) {
            case LdapConnection.STATUS_INSERTED:
              incrementLinesOutput();
              break;
            case LdapConnection.STATUS_UPDATED:
              incrementLinesUpdated();
              break;
            default:
              incrementLinesSkipped();
              break;
          }
          break;
        case LdapOutputMeta.OPERATION_TYPE_UPDATE:
          status =
              data.connection.update(
                  dn, data.fieldsAttribute, data.attributes, meta.isFailIfNotExist());
          if (status == LdapConnection.STATUS_UPDATED) {
            incrementLinesUpdated();
          } else {
            incrementLinesSkipped();
          }
          break;
        case LdapOutputMeta.OPERATION_TYPE_ADD:
          status =
              data.connection.add(
                  dn,
                  data.fieldsAttribute,
                  data.attributes,
                  data.separator,
                  meta.isFailIfNotExist());
          if (status == LdapConnection.STATUS_ADDED) {
            incrementLinesUpdated();
          } else {
            incrementLinesSkipped();
          }
          break;
        case LdapOutputMeta.OPERATION_TYPE_DELETE:
          status = data.connection.delete(dn, meta.isFailIfNotExist());
          if (status == LdapConnection.STATUS_DELETED) {
            incrementLinesUpdated();
          } else {
            incrementLinesSkipped();
          }
          break;
        case LdapOutputMeta.OPERATION_TYPE_RENAME:
          String oldDn = getInputRowMeta().getString(outputRowData, data.indexOfOldDNField);
          String newDn = getInputRowMeta().getString(outputRowData, data.indexOfNewDNField);

          data.connection.rename(oldDn, newDn, meta.isDeleteRDN());
          incrementLinesOutput();
          break;
        default:
          // Insert
          data.connection.insert(dn, data.fieldsAttribute, data.attributes, data.separator);
          incrementLinesOutput();
          break;
      }

      putRow(getInputRowMeta(), outputRowData); // copy row to output rowset(s);

      if (log.isRowLevel()) {
        logRowlevel(
            BaseMessages.getString(classFromResourcesPackage, "LdapOutput.log.ReadRow"),
            getInputRowMeta().getString(outputRowData));
      }

      if (checkFeedback(getLinesInput()) && log.isDetailed()) {
        logDetailed(
            BaseMessages.getString(classFromResourcesPackage, "LdapOutput.log.LineRow")
                + getLinesInput());
      }

      return true;

    } catch (Exception e) {

      boolean sendToErrorRow = false;
      String errorMessage = null;

      if (getTransformMeta().isDoingErrorHandling()) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError(
            BaseMessages.getString(
                classFromResourcesPackage, "LdapOutput.log.Exception", e.getMessage()));
        setErrors(1);
        logError(Const.getStackTracker(e));
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if (sendToErrorRow) {
        // Simply add this row to the error row
        putError(getInputRowMeta(), outputRowData, 1, errorMessage, null, "LdapOutput001");
      }
    }
    return true;
  }

  public boolean init() {
    if (super.init()) {
      try {
        // Define new LDAP connection
        data.connection = new LdapConnection(log, this, meta, null);

        // connect
        if (meta.isUseAuthentication()) {
          String username = resolve(meta.getUserName());
          String password =
              Encr.decryptPasswordOptionallyEncrypted( resolve(meta.getPassword()));
          data.connection.connect(username, password);
        } else {
          data.connection.connect();
        }
        data.separator = resolve(meta.getMultiValuedSeparator());
      } catch (Exception e) {
        logError(BaseMessages.getString(classFromResourcesPackage, "LdapOutput.Error.Init", e));
        stopAll();
        setErrors(1);
        return false;
      }
      return true;
    }
    return false;
  }

  public void dispose() {
    if (data.connection != null) {
      try {
        // Close connection
        data.connection.close();
      } catch (HopException e) {
        logError(
            BaseMessages.getString(
                classFromResourcesPackage, "LdapOutput.Exception.ErrorDisconecting", e.toString()));
      }
    }

    super.dispose();
  }
}
