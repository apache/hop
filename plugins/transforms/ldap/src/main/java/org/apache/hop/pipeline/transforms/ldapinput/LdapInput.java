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
package org.apache.hop.pipeline.transforms.ldapinput;

import java.util.HashSet;
import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Read LDAP Host, convert them to rows and writes these to one or more output streams.
 *
 * @author Samatar
 * @since 21-09-2007
 */
public class LdapInput extends BaseTransform<LdapInputMeta, LdapInputData>
    implements ITransform<LdapInputMeta, LdapInputData> {
  private static final Class<?> PKG = LdapInputMeta.class; // For Translator

  public LdapInput(
      TransformMeta transformMeta,
      LdapInputMeta meta,
      LdapInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    if (!data.dynamic && first) {

      first = false;

      // Create the output row meta-data
      data.outputRowMeta = new RowMeta();
      meta.getFields(
          data.outputRowMeta, getTransformName(), null, null, this, metadataProvider); // get the
      // metadata
      // populated

      // Create convert meta-data objects that will contain Date & Number formatters
      //
      data.convertRowMeta = data.outputRowMeta.cloneToType(IValueMeta.TYPE_STRING);

      // Search records once
      search(data.staticSearchBase, data.staticFilter);
    }

    Object[] outputRowData = null;

    try {
      outputRowData = getOneRow();

      if (outputRowData == null) {
        setOutputDone();
        return false;
      }

      putRow(data.outputRowMeta, outputRowData); // copy row to output rowset(s);

      if (log.isRowLevel()) {
        logRowlevel(
            BaseMessages.getString(PKG, "LdapInput.log.ReadRow"),
            data.outputRowMeta.getString(outputRowData));
      }

      if (checkFeedback(getLinesInput()) && log.isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "LdapInput.log.LineRow") + getLinesInput());
      }

      return true;

    } catch (Exception e) {
      boolean sendToErrorRow = false;
      String errorMessage = null;

      if (getTransformMeta().isDoingErrorHandling()) {
        sendToErrorRow = true;
        errorMessage = e.toString();
      } else {
        logError(BaseMessages.getString(PKG, "LdapInput.log.Exception", e.getMessage()));
        setErrors(1);
        logError(Const.getStackTracker(e));
        stopAll();
        setOutputDone(); // signal end to receiver(s)
        return false;
      }
      if (sendToErrorRow) {
        // Simply add this row to the error row
        putError(getInputRowMeta(), outputRowData, 1, errorMessage, null, "LDAPINPUT001");
      }
    }
    return true;
  }

  private boolean dynamicSearch() throws HopException {

    data.readRow = getRow();
    // Get row from input rowset & set row busy!
    if (data.readRow == null) {
      if (log.isDetailed()) {
        logDetailed(BaseMessages.getString(PKG, "LdapInput.Log.FinishedProcessing"));
      }
      setOutputDone();
      return false;
    }
    if (first) {
      first = false;

      if (meta.isDynamicSearch() && Utils.isEmpty(meta.getDynamicSearchFieldName())) {
        throw new HopException(
            BaseMessages.getString(PKG, "LdapInput.Error.DynamicSearchFieldMissing"));
      }
      if (meta.isDynamicFilter() && Utils.isEmpty(meta.getDynamicFilterFieldName())) {
        throw new HopException(
            BaseMessages.getString(PKG, "LdapInput.Error.DynamicFilterFieldMissing"));
      }

      // Create the output row meta-data
      data.nrIncomingFields = getInputRowMeta().size();
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(
          data.outputRowMeta,
          getTransformName(),
          null,
          null,
          this,
          metadataProvider); // get the metadata
      // populated

      // Create convert meta-data objects that will contain Date & Number formatters
      //
      data.convertRowMeta = data.outputRowMeta.cloneToType(IValueMeta.TYPE_STRING);

      if (meta.isDynamicSearch()) {
        data.indexOfSearchBaseField =
            getInputRowMeta().indexOfValue(meta.getDynamicSearchFieldName());
        if (data.indexOfSearchBaseField < 0) {
          // The field is unreachable !
          throw new HopException(
              BaseMessages.getString(
                  PKG, "LdapInput.Exception.CouldnotFindField", meta.getDynamicSearchFieldName()));
        }
      }
      if (meta.isDynamicFilter()) {
        data.indexOfFilterField = getInputRowMeta().indexOfValue(meta.getDynamicFilterFieldName());
        if (data.indexOfFilterField < 0) {
          // The field is unreachable !
          throw new HopException(
              BaseMessages.getString(
                  PKG, "LdapInput.Exception.CouldnotFindField", meta.getDynamicFilterFieldName()));
        }
      }
    } // end if

    String searchBase = data.staticSearchBase;
    if (data.indexOfSearchBaseField > 0) {
      // retrieve dynamic search base value
      searchBase = getInputRowMeta().getString(data.readRow, data.indexOfSearchBaseField);
    }
    String filter = data.staticFilter;
    if (data.indexOfFilterField >= 0) {
      // retrieve dynamic filter string
      filter = getInputRowMeta().getString(data.readRow, data.indexOfFilterField);
    }

    search(searchBase, filter);

    return true;
  }

  private Object[] getOneRow() throws HopException {

    if (data.dynamic) {
      while (data.readRow == null || (data.attributes = data.connection.getAttributes()) == null) {
        // no records to retrieve
        // we need to perform another search with incoming row
        if (!dynamicSearch()) {
          // we finished with incoming rows
          if (log.isDetailed()) {
            logDetailed(BaseMessages.getString(PKG, "LdapInput.Log.FinishedProcessing"));
          }
          return null;
        }
      }
    } else {
      // search base is static
      // just try to return records
      data.attributes = data.connection.getAttributes();
    }
    if (data.attributes == null) {
      // no more records
      return null;
    }
    return buildRow();
  }

  private Object[] buildRow() throws HopException {

    // Build an empty row based on the meta-data
    Object[] outputRowData = buildEmptyRow();

    if (data.dynamic) {
      // Reserve room for new row
      System.arraycopy(data.readRow, 0, outputRowData, 0, data.readRow.length);
    }

    try {

      // Execute for each Input field...
      for (int i = 0; i < meta.getInputFields().length; i++) {

        LdapInputField field = meta.getInputFields()[i];
        // Get attribute value
        int index = data.nrIncomingFields + i;
        Attribute attr = data.attributes.get(field.getRealAttribute());
        if (attr != null) {
          // Let's try to get value of this attribute
          outputRowData[index] = getAttributeValue(field, attr, index, outputRowData[index]);
        }

        // Do we need to repeat this field if it is null?
        if (field.isRepeated() && data.previousRow != null && outputRowData[index] == null) {
          outputRowData[index] = data.previousRow[index];
        }
      } // End of loop over fields...

      int fIndex = data.nrIncomingFields + data.nrFields;

      // See if we need to add the row number to the row...
      if (meta.isIncludeRowNumber() && !Utils.isEmpty(meta.getRowNumberField())) {
        outputRowData[fIndex] = new Long(data.rownr);
      }

      IRowMeta irow = getInputRowMeta();

      data.previousRow =
          irow == null ? outputRowData : irow.cloneRow(outputRowData); // copy it to make
      // surely the next transform doesn't change it in between...
      data.rownr++;

      incrementLinesInput();

    } catch (Exception e) {
      throw new HopException(BaseMessages.getString(PKG, "LdapInput.Exception.CanNotReadLDAP"), e);
    }
    return outputRowData;
  }

  private Object getAttributeValue(
      LdapInputField field, Attribute attr, int i, Object outputRowData) throws Exception {

    if (field.getType() == IValueMeta.TYPE_BINARY) {
      // It's a binary field
      // no need to convert, just return the value as it
      try {
        return attr.get();
      } catch ( ClassCastException e) {
        return attr.get().toString().getBytes();
      }
    }

    String retval = null;
    if (field.getReturnType() == LdapInputField.FETCH_ATTRIBUTE_AS_BINARY
        && field.getType() == IValueMeta.TYPE_STRING) {
      // Convert byte[] to string
      return LdapConnection.extractBytesAndConvertToString(attr, field.isObjectSid());
    }

    // extract as string
    retval = extractString(attr);

    // DO Trimming!
    switch (field.getTrimType()) {
      case LdapInputField.TYPE_TRIM_LEFT:
        retval = Const.ltrim(retval);
        break;
      case LdapInputField.TYPE_TRIM_RIGHT:
        retval = Const.rtrim(retval);
        break;
      case LdapInputField.TYPE_TRIM_BOTH:
        retval = Const.trim(retval);
        break;
      default:
        break;
    }

    // DO CONVERSIONS...
    //
    IValueMeta targetValueMeta = data.outputRowMeta.getValueMeta(i);
    IValueMeta sourceValueMeta = data.convertRowMeta.getValueMeta(i);
    return targetValueMeta.convertData(sourceValueMeta, retval);
  }

  private String extractString(Attribute attr) throws Exception {
    StringBuilder attrStr = new StringBuilder();
    for (NamingEnumeration<?> eattr = attr.getAll(); eattr.hasMore(); ) {
      if (attrStr.length() > 0) {
        attrStr.append(data.multiValuedFieldSeparator);
      }
      attrStr.append(eattr.next().toString());
    }
    return attrStr.toString();
  }

  private void connectServerLdap() throws HopException {
    // Limit returned attributes to user selection
    data.attrReturned = new String[meta.getInputFields().length];

    data.attributesBinary = new HashSet<>();
    // Get user selection attributes
    for (int i = 0; i < meta.getInputFields().length; i++) {
      LdapInputField field = meta.getInputFields()[i];
      // get real attribute name
      String name = resolve(field.getAttribute());
      field.setRealAttribute(name);

      // specify attributes to be returned in binary format
      if (field.getReturnType() == LdapInputField.FETCH_ATTRIBUTE_AS_BINARY) {
        data.attributesBinary.add(name);
      }

      data.attrReturned[i] = name;
    }

    // Define new LDAP connection
    data.connection = new LdapConnection(log, this, meta, data.attributesBinary);

    for (int i = 0; i < data.attrReturned.length; i++) {
      LdapInputField field = meta.getInputFields()[i];
      // Do we need to sort based on some attributes?
      if (field.isSortedKey()) {
        data.connection.addSortingAttributes(data.attrReturned[i]);
      }
    }

    if (meta.isUseAuthentication()) {
      String username = resolve(meta.getUserName());
      String password =
          Encr.decryptPasswordOptionallyEncrypted( resolve(meta.getPassword()));
      data.connection.connect(username, password);
    } else {
      data.connection.connect();
    }

    // Time Limit
    if (meta.getTimeLimit() > 0) {
      data.connection.setTimeLimit(meta.getTimeLimit() * 1000);
    }
    // Set the page size?
    if (meta.isPaging()) {
      data.connection.setPagingSize(Const.toInt( resolve(meta.getPageSize()), -1));
    }
  }

  private void search(String searchBase, String filter) throws HopException {

    // Set the filter string. The more exact of the search string
    // Set the Search base.This is the place where the search will
    data.connection.search(
        searchBase, filter, meta.getRowLimit(), data.attrReturned, meta.getSearchScope());
  }

  /**
   * Build an empty row based on the meta-data...
   *
   * @return
   */
  private Object[] buildEmptyRow() {
    Object[] rowData = RowDataUtil.allocateRowData(data.outputRowMeta.size());

    return rowData;
  }

  @Override
  public boolean init() {
    if (super.init()) {
      data.rownr = 1L;
      // Get multi valued field separator
      data.multiValuedFieldSeparator = resolve(meta.getMultiValuedSeparator());
      data.nrFields = meta.getInputFields().length;
      // Set the filter string
      data.staticFilter = resolve(meta.getFilterString());
      // Set the search base
      data.staticSearchBase = resolve(meta.getSearchBase());

      data.dynamic = (meta.isDynamicSearch() || meta.isDynamicFilter());
      try {
        // Try to connect to LDAP server
        connectServerLdap();

        return true;

      } catch (Exception e) {
        logError(BaseMessages.getString(PKG, "LdapInput.ErrorInit", e.toString()));
        stopAll();
        setErrors(1);
      }
    }
    return false;
  }

  @Override
  public void dispose() {

    if (data.connection != null) {
      try {
        // close connection
        data.connection.close();
      } catch (HopException e) {
        logError(
            BaseMessages.getString(PKG, "LdapInput.Exception.ErrorDisconecting", e.toString()));
      }
    }
    data.attributesBinary = null;

    super.dispose();
  }
}
