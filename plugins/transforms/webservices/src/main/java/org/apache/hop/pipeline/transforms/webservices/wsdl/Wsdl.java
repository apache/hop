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

package org.apache.hop.pipeline.transforms.webservices.wsdl;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serial;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import javax.wsdl.Binding;
import javax.wsdl.Definition;
import javax.wsdl.Operation;
import javax.wsdl.Port;
import javax.wsdl.PortType;
import javax.wsdl.Service;
import javax.wsdl.WSDLException;
import javax.wsdl.extensions.ExtensionRegistry;
import javax.wsdl.factory.WSDLFactory;
import javax.wsdl.xml.WSDLLocator;
import javax.wsdl.xml.WSDLReader;
import javax.xml.namespace.QName;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Document;

/** Wsdl abstraction. */
public final class Wsdl implements java.io.Serializable {
  @Serial private static final long serialVersionUID = 1L;
  public static final String CONST_COULD_NOT_LOAD_WSDL_FILE = "Could not load WSDL file: ";
  private Port port;
  private final Definition wsdlDefinition;
  private final Service service;
  private final WsdlTypes wsdlTypes;
  private HashMap<String, WsdlOperation> operationHashMap;

  /**
   * Loads and parses the specified WSDL file.
   *
   * @param wsdlURI URI of a WSDL file.
   * @param serviceQName Name of the service in the WSDL, if null default to first service in WSDL.
   * @param portName The service port name, if null default to first port in service.
   */
  public Wsdl(URI wsdlURI, QName serviceQName, String portName) {
    this(wsdlURI, serviceQName, portName, null, null);
  }

  public Wsdl(URI wsdlURI, QName serviceQName, String portName, String username, String password) {
    this(wsdlURI.toString(), null, serviceQName, portName, username, password);
  }

  /**
   * Loads and parses the WSDL file at the specified location.
   *
   * @param wsdlLocation An http(s) URL, or any file name Hop VFS understands: a plain path, a file:
   *     URL or a VFS location like s3://.
   * @param variables The variables of the caller, used to find the named VFS connections. Can be
   *     null.
   * @param serviceQName Name of the service in the WSDL, if null default to first service in WSDL.
   * @param portName The service port name, if null default to first port in service.
   * @param username to use for HTTP authentication
   * @param password to use for HTTP authentication
   */
  public Wsdl(
      String wsdlLocation,
      IVariables variables,
      QName serviceQName,
      String portName,
      String username,
      String password) {

    try {
      wsdlDefinition = parse(wsdlLocation, variables, username, password);
    } catch (WSDLException | HopException e) {
      throw new HopRuntimeException(
          CONST_COULD_NOT_LOAD_WSDL_FILE
              + wsdlLocation
              + " : "
              + Const.NVL(e.getMessage(), "").trim(),
          e);
    }
    if (serviceQName == null) {
      service = (Service) wsdlDefinition.getServices().values().iterator().next();
    } else {
      service = wsdlDefinition.getService(serviceQName);
      if (service == null) {
        throw new IllegalArgumentException(
            "Service: " + serviceQName + " is not defined in the WSDL file " + wsdlLocation);
      }
    }

    if (portName == null) {
      port = getSoapPort(service.getPorts().values());
    } else {
      port = service.getPort(portName);
      if (port == null) {
        throw new IllegalArgumentException(
            "Port: " + portName + " is not defined in the service: " + serviceQName);
      } else {
        port = service.getPort(portName);
      }
    }

    wsdlTypes = new WsdlTypes(wsdlDefinition);
    operationHashMap = new HashMap<>();
  }

  /**
   * Returns the first Soap port from the passed collection of Ports.
   *
   * @param portCollection
   * @return
   */
  private Port getSoapPort(Collection<?> portCollection) {
    Port soapPort = null;
    Iterator<?> iterator = portCollection.iterator();
    while (iterator.hasNext()) {
      Port tempPort = (Port) iterator.next();
      if (WsdlUtils.isSoapPort(tempPort)) {
        soapPort = tempPort;
        break;
      }
    }
    return soapPort;
  }

  /**
   * Loads and parses the specified WSDL file.
   *
   * @param wsdlLocator A javax.wsdl.WSDLLocator instance.
   * @param serviceQName Name of the service in the WSDL.
   * @param portName The service port name.
   */
  public Wsdl(WSDLLocator wsdlLocator, QName serviceQName, String portName) {
    this(wsdlLocator, serviceQName, portName, null, null);
  }

  public Wsdl(
      WSDLLocator wsdlLocator,
      QName serviceQName,
      String portName,
      String username,
      String password) {

    // load and parse the WSDL
    try {
      wsdlDefinition = parse(wsdlLocator);
    } catch (WSDLException e) {
      throw new HopRuntimeException(CONST_COULD_NOT_LOAD_WSDL_FILE + e.getMessage(), e);
    }

    service = wsdlDefinition.getService(serviceQName);
    if (service == null) {
      throw new IllegalArgumentException(
          "Service: " + serviceQName + " is not defined in the WSDL file.");
    }

    port = service.getPort(portName);
    if (port == null) {
      throw new IllegalArgumentException(
          "Port: " + portName + " is not defined in the service: " + serviceQName);
    }

    wsdlTypes = new WsdlTypes(wsdlDefinition);
    operationHashMap = new HashMap<>();
  }

  /**
   * Get the WsdlComplexTypes instance of this wsdl. WsdlComplex types provides type information for
   * named complextypes defined in the wsdl's &lt;types&gt; section.
   *
   * @return WsdlComplexTypes instance.
   */
  public WsdlComplexTypes getComplexTypes() {
    return wsdlTypes.getNamedComplexTypes();
  }

  /**
   * Find the specified operation in the WSDL definition.
   *
   * @param operationName Name of operation to find.
   * @return A WsdlOperation instance, null if operation can not be found in WSDL.
   */
  public WsdlOperation getOperation(String operationName) throws HopTransformException {

    // is the operation in the cache?
    if (operationHashMap.containsKey(operationName)) {
      return operationHashMap.get(operationName);
    }

    Binding b = port.getBinding();
    PortType pt = b.getPortType();
    Operation op = pt.getOperation(operationName, null, null);
    if (op != null) {
      try {
        WsdlOperation wop = new WsdlOperation(b, op, wsdlTypes);
        // cache the operation
        operationHashMap.put(operationName, wop);
        return wop;
      } catch (HopException kse) {
        LogChannel.GENERAL.logError(
            "Could not retrieve WSDL Operator for operation name: " + operationName);
        throw new HopTransformException(
            "Could not retrieve WSDL Operator for operation name: " + operationName, kse);
      }
    }
    return null;
  }

  /**
   * Get a list of all operations defined in this WSDL.
   *
   * @return List of WsdlOperations.
   */
  public List<WsdlOperation> getOperations() throws HopTransformException {

    List<WsdlOperation> opList = new ArrayList<>();
    PortType pt = port.getBinding().getPortType();

    List<Operation> operations = pt.getOperations();
    for (Operation value : operations) {
      WsdlOperation operation = getOperation(value.getName());
      if (operation != null) {
        opList.add(operation);
      }
    }
    return opList;
  }

  /**
   * Get the name of the current port.
   *
   * @return Name of the current port.
   */
  public String getPortName() {
    return port.getName();
  }

  /**
   * Get the PortType name for the service which has been specified by serviceName and portName at
   * construction time.
   *
   * @return QName of the PortType.
   */
  public QName getPortTypeQName() {

    Binding b = port.getBinding();
    return b.getPortType().getQName();
  }

  /**
   * Get the service endpoint.
   *
   * @return String containing the service endpoint.
   */
  public String getServiceEndpoint() {
    return WsdlUtils.getSOAPAddress(port);
  }

  /**
   * Get the name of this service.
   *
   * @return Service name.
   */
  public String getServiceName() {
    return service.getQName().getLocalPart();
  }

  /**
   * Get the target namespace for the WSDL.
   *
   * @return The targetNamespace
   */
  public String getTargetNamespace() {
    return wsdlDefinition.getTargetNamespace();
  }

  /**
   * Change the port of the service.
   *
   * @param portName The new port name.
   * @throws IllegalArgumentException if port name is not defined in WSDL.
   */
  public void setPort(QName portName) {

    Port tempPort = service.getPort(portName.getLocalPart());
    if (tempPort == null) {
      throw new IllegalArgumentException(
          "Port name: '" + portName + "' was not found in the WSDL file.");
    }

    this.port = tempPort;
    operationHashMap.clear();
  }

  /**
   * Get a WSDLReader.
   *
   * @return WSDLReader.
   * @throws WSDLException on error.
   */
  private WSDLReader getReader() throws WSDLException {

    WSDLFactory wsdlFactory = WSDLFactory.newInstance();
    WSDLReader wsdlReader = wsdlFactory.newWSDLReader();
    ExtensionRegistry registry = wsdlFactory.newPopulatedExtensionRegistry();
    wsdlReader.setExtensionRegistry(registry);
    wsdlReader.setFeature("javax.wsdl.verbose", true);
    wsdlReader.setFeature("javax.wsdl.importDocuments", true);
    return wsdlReader;
  }

  /**
   * Load and parse the WSDL file using the wsdlLocator.
   *
   * @param wsdlLocator A WSDLLocator instance.
   * @return wsdl Definition.
   * @throws WSDLException on error.
   */
  private Definition parse(WSDLLocator wsdlLocator) throws WSDLException {
    return getReader().readWSDL(wsdlLocator);
  }

  /**
   * Load and parse the WSDL file at the specified location.
   *
   * @param wsdlLocation http(s) URL or Hop VFS file name of the WSDL file.
   * @param variables to find the named VFS connections with, can be null
   * @param username to use for HTTP authentication
   * @param password to use for HTTP authentication
   * @return wsdl Definition
   * @throws WSDLException on error.
   */
  private Definition parse(
      String wsdlLocation, IVariables variables, String username, String password)
      throws WSDLException, HopException {
    if (StringUtils.isBlank(wsdlLocation)) {
      throw new HopException("No WSDL location was specified");
    }
    String location = wsdlLocation.trim();
    WSDLReader wsdlReader = getReader();

    // Imports are resolved relative to the WSDL's own URI and read through the same locator, so
    // they come from wherever the WSDL came from: over http(s), or from any file system Hop VFS
    // knows about. Everything it opens is closed once the WSDL has been read, failure or not.
    //
    try (HopVfsWsdlLocator locator =
        new HopVfsWsdlLocator(baseUri(location, variables), variables, username, password)) {
      Document doc;
      try (InputStream wsdlStream = locator.openBase()) {
        doc = XmlHandler.loadXmlFile(wsdlStream, locator.getBaseURI(), false, true);
      } catch (IOException e) {
        throw new HopException(e);
      }
      if (doc == null) {
        throw new HopException("Unable to get document.");
      }
      return wsdlReader.readWSDL(locator, doc.getDocumentElement());
    } catch (HopRuntimeException e) {
      // An import the locator could not read. Unwrapped, so the message says which.
      throw new HopException(e.getMessage(), e.getCause() == null ? e : e.getCause());
    }
  }

  /**
   * The absolute URI of the WSDL: an http(s) URL as it stands, anything else, a plain path like the
   * file dialog hands out, a file: URL or any other location Hop VFS knows about, as Hop VFS names
   * it.
   */
  private static String baseUri(String location, IVariables variables) throws HopException {
    if (isHttpLocation(location)) {
      return location;
    }
    try (FileObject wsdlFile =
        variables == null
            ? HopVfs.getFileObject(location)
            : HopVfs.getFileObject(location, variables)) {
      return wsdlFile.getName().getURI();
    } catch (FileSystemException e) {
      throw new HopException("Unable to read WSDL file " + location, e);
    }
  }

  static boolean isHttpLocation(String wsdlLocation) {
    return Strings.CI.startsWithAny(wsdlLocation, "http://", "https://");
  }

  /**
   * Returns this objects WSDL types.
   *
   * @return WsdlTepes
   */
  public WsdlTypes getWsdlTypes() {
    return this.wsdlTypes;
  }
}
