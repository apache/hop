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

import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import javax.naming.NameClassPair;
import javax.naming.NameNotFoundException;
import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.DirContext;
import javax.naming.directory.ModificationItem;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.Control;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.PagedResultsControl;
import javax.naming.ldap.PagedResultsResponseControl;
import javax.naming.ldap.SortControl;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;

public class LdapConnection {
  private static Class<?> classFromResourcesPackage =
      LdapInputMeta.class; // For Translator

  public static final int SEARCH_SCOPE_OBJECT_SCOPE = 0;

  public static final int SEARCH_SCOPE_ONELEVEL_SCOPE = 1;

  public static final int SEARCH_SCOPE_SUBTREE_SCOPE = 2;

  public static final int DEFAULT_PORT = 389;

  public static final String DEFAUL_FILTER_STRING = "objectclass=*";

  public static final int STATUS_SKIPPED = 0;

  public static final int STATUS_INSERTED = 1;

  public static final int STATUS_UPDATED = 2;

  public static final int STATUS_DELETED = 3;

  public static final int STATUS_ADDED = 4;

  private final ILogChannel log;

  private String searchBase;

  private String filter;

  private SearchControls controls;

  private int timeLimit;

  private int pagingSize;

  private NamingEnumeration<SearchResult> results;

  private List<String> sortingAttributes;

  private String[] sortingAttributesKeys;

  private LdapProtocol protocol;

  /** Construct a new LDAP Connection */
  public LdapConnection(
      ILogChannel logInterface,
      IVariables variables,
      ILdapMeta meta,
      Collection<String> binaryAttributes)
      throws HopException {
    this.log = logInterface;
    protocol =
        new LdapProtocolFactory(logInterface).createLdapProtocol(variables, meta, binaryAttributes);
    this.sortingAttributes = new ArrayList<>();
  }

  /**
   * Connect to LDAP server
   *
   * @throws HopException
   */
  public void connect() throws HopException {
    connect(null, null);
  }

  /**
   * Connect to LDAP server
   *
   * @param username : username
   * @param password : password
   * @throws HopException
   */
  public void connect(String username, String password) throws HopException {
    protocol.connect(username, password);
  }

  public void setSortingAttributesKeys(String[] value) {
    this.sortingAttributesKeys = value;
  }

  private String[] getSortingAttributesKeys() {
    return this.sortingAttributesKeys;
  }

  public void addSortingAttributes(String value) {
    this.sortingAttributes.add(value);
  }

  public List<String> getSortingAttributes() {
    return this.sortingAttributes;
  }

  private boolean isSortingAttributes() {
    return (!this.sortingAttributes.isEmpty());
  }

  private void setFilter(String filter) {
    this.filter = filter;
  }

  private String getFilter() {
    return this.filter;
  }

  private void setSearchBase(String searchBase) {
    this.searchBase = searchBase;
  }

  private String getSearchBase() {
    return this.searchBase;
  }

  public void setTimeLimit(int timeLimit) {
    this.timeLimit = timeLimit;
  }

  public int getTimeLimit() {
    return this.timeLimit;
  }

  public void setPagingSize(int value) {
    this.pagingSize = value;
  }

  private int getPagingSize() {
    return this.pagingSize;
  }

  private boolean isPagingUsed() {
    return (getPagingSize() > 0);
  }

  public void search(
      String searchBase, String filter, int limitRows, String[] attributeReturned, int searchScope)
      throws HopException {

    // Set the Search base.This is the place where the search will
    setSearchBase(searchBase);
    setFilter(Const.NVL(correctFilter(filter), DEFAUL_FILTER_STRING));
    try {

      if (Utils.isEmpty(getSearchBase())) {
        // get Search Base
        Attributes attrs = getInitialContext().getAttributes("", new String[] {"namingContexts"});
        Attribute attr = attrs.get("namingContexts");

        setSearchBase(attr.get().toString());
        if (log.isDetailed()) {
          log.logDetailed(
              BaseMessages.getString(
                  classFromResourcesPackage, "LdapInput.SearchBaseFound", getSearchBase()));
        }
      }

      this.controls = new SearchControls();
      if (limitRows > 0) {
        this.controls.setCountLimit(limitRows);
      }

      // Time Limit
      if (getTimeLimit() > 0) {
        this.controls.setTimeLimit(getTimeLimit() * 1000);
      }

      // Specify the attributes to return
      if (attributeReturned != null) {
        this.controls.setReturningAttributes(attributeReturned);
      }

      // Specify the search scope
      switch (searchScope) {
        case SEARCH_SCOPE_OBJECT_SCOPE:
          this.controls.setSearchScope(SearchControls.OBJECT_SCOPE);
          break;
        case SEARCH_SCOPE_ONELEVEL_SCOPE:
          this.controls.setSearchScope(SearchControls.ONELEVEL_SCOPE);
          break;
        default:
          this.controls.setSearchScope(SearchControls.SUBTREE_SCOPE);
          break;
      }

      Control ctlp = null;
      Control ctlk = null;
      int nrCtl = 0;

      // Set the sort search?
      if (isSortingAttributes()) {
        // Create a sort control that sorts based on attributes
        setSortingAttributesKeys(
            getSortingAttributes().toArray(new String[getSortingAttributes().size()]));
        ctlk = new SortControl(getSortingAttributesKeys(), Control.NONCRITICAL);
        nrCtl++;
        if (log.isDebug()) {
          log.logDebug(
              BaseMessages.getString(
                  "LdapInput.Log.SortingKeys", Arrays.toString(getSortingAttributesKeys())));
        }
      }

      // Set the page size?
      if (isPagingUsed()) {
        // paging is activated
        // Request the paged results control
        ctlp = new PagedResultsControl(getPagingSize(), Control.CRITICAL);
        nrCtl++;
        if (log.isDebug()) {
          log.logDebug(
              BaseMessages.getString("LdapInput.Log.PageSize", String.valueOf(getPagingSize())));
        }
      }

      if (nrCtl > 0) {
        Control[] ctls = new Control[nrCtl];
        int index = 0;
        if (ctlk != null) {
          ctls[index] = ctlk;
          index++;
        }
        if (ctlp != null) {
          ctls[index] = ctlp;
        }
        getInitialContext().setRequestControls(ctls);
      }
      // Search for objects using the filter
      this.results = getInitialContext().search(getSearchBase(), getFilter(), getSearchControls());

    } catch (Exception e) {
      throw new HopException(BaseMessages.getString("LdapConnection.Error.Search"), e);
    }
  }

  public int delete(String dn, boolean checkEntry) throws HopException {
    try {

      if (checkEntry) {
        // First Check entry
        getInitialContext().lookup(dn);
      }
      // The entry exists
      getInitialContext().destroySubcontext(dn);
      if (log.isDebug()) {
        log.logDebug(
            BaseMessages.getString(classFromResourcesPackage, "LDAPinput.Exception.Deleted", dn));
      }
      return STATUS_DELETED;
    } catch (NameNotFoundException n) {
      // The entry is not found
      if (checkEntry) {
        throw new HopException(
            BaseMessages.getString(
                classFromResourcesPackage, "LdapConnection.Error.Deleting.NameNotFound", dn),
            n);
      }
      return STATUS_SKIPPED;
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(classFromResourcesPackage, "LdapConnection.Error.Delete", dn), e);
    }
  }

  public int update(String dn, String[] attributes, String[] values, boolean checkEntry)
      throws HopException {
    try {
      int nrAttributes = attributes.length;
      ModificationItem[] mods = new ModificationItem[nrAttributes];
      for (int i = 0; i < nrAttributes; i++) {
        // Define attribute
        Attribute mod = new BasicAttribute(attributes[i], values[i]);
        if (log.isDebug()) {
          log.logDebug(
              BaseMessages.getString(
                  classFromResourcesPackage,
                  "LdapConnection.Update.Attribute",
                  attributes[i],
                  values[i]));
        }
        // Save update action on attribute
        mods[i] = new ModificationItem(DirContext.REPLACE_ATTRIBUTE, mod);
      }
      // We have all requested attribute
      // let's update now
      getInitialContext().modifyAttributes(dn, mods);
      return STATUS_UPDATED;
    } catch (NameNotFoundException n) {
      // The entry is not found
      if (checkEntry) {
        throw new HopException(
            BaseMessages.getString(
                classFromResourcesPackage, "LdapConnection.Error.Deleting.NameNotFound", dn),
            n);
      }
      return STATUS_SKIPPED;
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(classFromResourcesPackage, "LdapConnection.Error.Update", dn), e);
    }
  }

  public int add(
      String dn,
      String[] attributes,
      String[] values,
      String multValuedSeparator,
      boolean checkEntry)
      throws HopException {
    try {
      Attributes attrs = buildAttributes(dn, attributes, values, multValuedSeparator);
      // We had all attributes
      getInitialContext().modifyAttributes(dn, DirContext.ADD_ATTRIBUTE, attrs);
      return STATUS_ADDED;
    } catch (NameNotFoundException n) {
      // The entry is not found
      if (checkEntry) {
        throw new HopException(
            BaseMessages.getString(
                classFromResourcesPackage, "LdapConnection.Error.Deleting.NameNotFound", dn),
            n);
      }
      return STATUS_SKIPPED;
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(classFromResourcesPackage, "LdapConnection.Error.Add", dn), e);
    }
  }

  /**
   * Insert record in LDAP based on DN
   *
   * @param dn : Distinguished Name (Key for lookup)
   * @param attributes : contains all the attributes to set for insert
   * @param values : contains all the values for attributes
   * @param multValuedSeparator : multi-valued attributes separator
   * @throws HopException
   */
  public void insert(String dn, String[] attributes, String[] values, String multValuedSeparator)
      throws HopException {
    try {

      Attributes attrs = buildAttributes(dn, attributes, values, multValuedSeparator);
      // We had all attributes
      // Let's insert now
      getInitialContext().createSubcontext(dn, attrs);

    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(classFromResourcesPackage, "LdapConnection.Error.Insert", dn), e);
    }
  }

  /**
   * Upsert record in LDAP First we will check if the entry exist based on DN If we can not find it,
   * we will create it otherwise, we will perform an update
   *
   * @param dn : Distinguished Name (Key for lookup)
   * @param attributes : contains all the attributes to set for insert
   * @param values : contains all the values for attributes
   * @param attributesToUpdate : contains attributes to update
   * @param valuesToUpdate : contains values for attributes to update
   * @param multValuedSeparator : multi-valued attributes separator
   * @return status : STATUS_INSERTED, STATUS_UPDATED or STATUS_SKIPPED
   * @throws HopException
   */
  public int upsert(
      String dn,
      String[] attributes,
      String[] values,
      String[] attributesToUpdate,
      String[] valuesToUpdate,
      String multValuedSeparator)
      throws HopException {

    try {

      boolean found = false;
      try {
        getInitialContext().getAttributes(dn);
        found = true;
      } catch (NameNotFoundException n) {
        Attributes attrs = buildAttributes(dn, attributes, values, multValuedSeparator);
        getInitialContext().createSubcontext(dn, attrs);
        return STATUS_INSERTED;
      }

      if (found && attributesToUpdate != null && attributesToUpdate.length > 0) {
        // The entry already exist
        // let's update
        Attributes attrs =
            buildAttributes(dn, attributesToUpdate, valuesToUpdate, multValuedSeparator);
        getInitialContext().modifyAttributes(dn, DirContext.REPLACE_ATTRIBUTE, attrs);
        return STATUS_UPDATED;
      }

    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(classFromResourcesPackage, "LdapConnection.Error.Upsert", dn), e);
    }
    return STATUS_SKIPPED;
  }

  private Attributes buildAttributes(
      String dn, String[] attributes, String[] values, String multValuedSeparator) {
    Attributes attrs = new javax.naming.directory.BasicAttributes(true);
    int nrAttributes = attributes.length;
    for (int i = 0; i < nrAttributes; i++) {
      if (!Utils.isEmpty(values[i])) {
        // We have a value
        String value = values[i].trim();
        if (multValuedSeparator != null && value.indexOf(multValuedSeparator) > 0) {
          Attribute attr = new BasicAttribute(attributes[i]);
          for (String attribute : value.split(multValuedSeparator)) {
            attr.add(attribute);
          }
          attrs.put(attr);
        } else {
          attrs.put(attributes[i], value);
        }
      }
    }
    return attrs;
  }

  /**
   * Rename an entry
   *
   * @param oldDn Distinguished name of the entry to rename
   * @param newDn target Distinguished name (new)
   * @param deleteRDN To specify whether you want to keep the old name attribute when you use rename
   *     entry true : do not keep the old value (defaut) false : keep the old value as an attribute
   * @throws HopException
   */
  public void rename(String oldDn, String newDn, boolean deleteRDN) throws HopException {
    try {
      if (!deleteRDN) {
        // Keep the old dn as attribute
        getInitialContext().removeFromEnvironment("java.naming.ldap.deleteRDN");
      }
      Map<String, Attributes> childs = new HashMap<>();
      List<String> paths = new ArrayList<>();

      getPaths(oldDn, childs, paths);

      // Destroy sub contexts
      for (String childName : paths) {
        getInitialContext().destroySubcontext(childName);
      }

      // Rename entry
      try {
        getInitialContext().rename(oldDn, newDn);
      } catch (Exception e) {
        // something goes wrong
        // re attached removed sub contexts
        for (int i = paths.size(); i > 0; i--) {
          getInitialContext().createSubcontext(paths.get(i - 1), childs.get(paths.get(i - 1)));
        }
        throw e;
      }

      // attach sub context
      List<String> newpaths = new ArrayList<>();
      for (String childName : paths) {
        newpaths.add(childName.replaceAll(oldDn, newDn));
      }

      for (int i = newpaths.size(); i > 0; i--) {
        getInitialContext().createSubcontext(newpaths.get(i - 1), childs.get(paths.get(i - 1)));
      }

    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(
              classFromResourcesPackage, "LdapConnection.Error.Renaming", oldDn, newDn),
          e);
    } finally {
      try {
        if (!deleteRDN) {
          // Delete the old dn as attribute
          // switch back to default value
          getInitialContext().addToEnvironment("java.naming.ldap.deleteRDN", "true");
        }
      } catch (Exception e) {
        // Ignore errors
      }
    }
  }

  @SuppressWarnings("rawtypes")
  private void getPaths(String rootName, Map<String, Attributes> childs, List<String> paths)
      throws Exception {
    NamingEnumeration ne = getInitialContext().list(rootName);
    while (ne.hasMore()) {
      NameClassPair nameCP = (NameClassPair) ne.next();
      childs.put(
          nameCP.getName() + "," + rootName,
          getInitialContext().getAttributes(nameCP.getName() + "," + rootName));
      getPaths(nameCP.getName() + "," + rootName, childs, paths);
      paths.add(nameCP.getName() + "," + rootName);
    }
  }

  /**
   * Close the LDAP connection
   *
   * @throws HopException
   */
  public void close() throws HopException {
    if (protocol != null) {
      protocol.close();
      protocol = null;
    }
    if (results != null) {
      results = null;
    }
  }

  public Attributes getAttributes() throws HopException {

    byte[] cookie = null;
    while (!getSearchResult().hasMoreElements()) {
      if (isPagingUsed()) {
        // we are using paging...
        // we need here to check the response controls
        // and pass back cookie to next page
        try {
          // examine response controls
          Control[] rc = getInitialContext().getResponseControls();
          if (rc != null) {
            for (int i = 0; i < rc.length; i++) {
              if (rc[i] instanceof PagedResultsResponseControl) {
                PagedResultsResponseControl prc = (PagedResultsResponseControl) rc[i];
                cookie = prc.getCookie();
              }
            }
          }
          // pass the cookie back for the next page
          if (isSortingAttributes()) {
            getInitialContext()
                .setRequestControls(
                    new Control[] {
                      new SortControl(getSortingAttributesKeys(), Control.NONCRITICAL),
                      new PagedResultsControl(getPagingSize(), cookie, Control.CRITICAL)
                    });
          } else {
            getInitialContext()
                .setRequestControls(
                    new Control[] {
                      new PagedResultsControl(getPagingSize(), cookie, Control.CRITICAL)
                    });
          }
          if ((cookie != null) && (cookie.length != 0)) {
            // get search result for the page
            this.results =
                getInitialContext().search(getSearchBase(), getFilter(), getSearchControls());
          } else {
            return null;
          }

        } catch (Exception e) {
          throw new HopException(
              BaseMessages.getString(classFromResourcesPackage, "LdapInput.Exception.ErrorPaging"),
              e);
        }

        while (!getSearchResult().hasMoreElements()) {
          return null;
        }
      } else {
        // User do not want to use paging
        // we have already returned all the result
        return null;
      }
    }

    try {
      SearchResult searchResult = getSearchResult().next();
      Attributes results = searchResult.getAttributes();
      results.put("dn", searchResult.getNameInNamespace());
      return results;
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(
              classFromResourcesPackage, "LdapConnection.Exception.GettingAttributes"),
          e);
    }
  }

  private InitialLdapContext getInitialContext() {
    return protocol.getCtx();
  }

  private SearchControls getSearchControls() {
    return this.controls;
  }

  private NamingEnumeration<SearchResult> getSearchResult() {
    return this.results;
  }

  /**
   * Remove CR and LF from filter string
   *
   * @param filter
   * @return corrected filter
   */
  private static String correctFilter(String filter) {
    return Utils.isEmpty(filter) ? "" : filter.replaceAll("(\\r|\\n)", "");
  }

  public static String extractBytesAndConvertToString(Attribute attr, boolean isSID)
      throws Exception {
    byte[] b;
    try {
      b = (byte[]) attr.get();
    } catch (Exception e) {
      // Get bytes from String
      b = attr.get().toString().getBytes();
    }
    if (isSID) {
      return getSIDAsString(b);
    } else {
      return byteToHexEncode(b);
    }
  }

  /**
   * Convert the SID into string format
   *
   * @param SID
   * @return String representation of SID
   */
  private static String getSIDAsString(byte[] SID) {
    long version;
    long authority;
    long count;
    long rid;
    String strSID;
    strSID = "S";
    version = SID[0];
    strSID = strSID + "-" + Long.toString(version);
    authority = SID[4];
    for (int i = 0; i < 4; i++) {
      authority <<= 8;
      authority += SID[4 + i] & 0xFF;
    }
    strSID = strSID + "-" + Long.toString(authority);
    count = SID[2];
    count <<= 8;
    count += SID[1] & 0xFF;
    for (int j = 0; j < count; j++) {
      rid = SID[11 + (j * 4)] & 0xFF;
      for (int k = 1; k < 4; k++) {
        rid <<= 8;
        rid += SID[11 - k + (j * 4)] & 0xFF;
      }
      strSID = strSID + "-" + Long.toString(rid);
    }
    return strSID;
  }

  /**
   * Converts the GUID to a readable string format
   *
   * @param inArr
   * @return the formatted GUID
   */
  private static String byteToHexEncode(byte[] inArr) {
    StringBuilder guid = new StringBuilder();
    for (int i = 0; i < inArr.length; i++) {
      StringBuilder dblByte = new StringBuilder(Integer.toHexString(inArr[i] & 0xff));
      if (dblByte.length() == 1) {
        guid.append("0");
      }
      guid.append(dblByte);
    }
    return guid.toString();
  }

  public RowMeta getFields(String searchBase) throws HopException {
    RowMeta fields = new RowMeta();
    List<String> fieldsl = new ArrayList<>();
    try {
      search(searchBase, null, 0, null, SEARCH_SCOPE_SUBTREE_SCOPE);
      Attributes attributes = null;
      fieldsl = new ArrayList<>();
      while ((attributes = getAttributes()) != null) {

        NamingEnumeration<? extends Attribute> ne = attributes.getAll();

        while (ne.hasMore()) {
          Attribute attr = ne.next();
          String fieldName = attr.getID();
          if (!fieldsl.contains(fieldName)) {
            fieldsl.add(fieldName);

            String attributeValue = attr.get().toString();
            int valueType;

            if (StringUtil.IsDate(attributeValue, "yy-mm-dd")) {
              valueType = IValueMeta.TYPE_DATE;
            } else if (StringUtil.IsInteger(attributeValue)) {
              valueType = IValueMeta.TYPE_INTEGER;
            } else if (StringUtil.IsNumber(attributeValue)) {
              valueType = IValueMeta.TYPE_NUMBER;
            } else {
              valueType = IValueMeta.TYPE_STRING;
            }

            IValueMeta value = ValueMetaFactory.createValueMeta(fieldName, valueType);
            fields.addValueMeta(value);
          }
        }
      }
      return fields;
    } catch (Exception e) {
      throw new HopException(
          BaseMessages.getString(
              classFromResourcesPackage, "LdapConnection.Error.RetrievingFields"));
    }
  }
}
