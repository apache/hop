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
import java.io.Serial;
import java.net.MalformedURLException;
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
import org.apache.hop.core.HttpProtocol;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.http.auth.AuthenticationException;
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
  private URI wsdlURI = null;

  /**
   * Loads and parses the specified WSDL file.
   *
   * @param wsdlURI URI of a WSDL file.
   * @param serviceQName Name of the service in the WSDL, if null default to first service in WSDL.
   * @param portName The service port name, if null default to first port in service.
   */
  public Wsdl(URI wsdlURI, QName serviceQName, String portName) throws AuthenticationException {
    this(wsdlURI, serviceQName, portName, null, null);
  }

  public Wsdl(URI wsdlURI, QName serviceQName, String portName, String username, String password)
      throws AuthenticationException {

    this.wsdlURI = wsdlURI;
    try {
      wsdlDefinition = parse(wsdlURI, username, password);
    } catch (AuthenticationException ae) {
      // throw this again since HopException is catching it
      throw ae;
    } catch (WSDLException | HopException e) {
      throw new RuntimeException(CONST_COULD_NOT_LOAD_WSDL_FILE + e.getMessage(), e);
    }
    if (serviceQName == null) {
      service = (Service) wsdlDefinition.getServices().values().iterator().next();
    } else {
      service = wsdlDefinition.getService(serviceQName);
      if (service == null) {
        throw new IllegalArgumentException(
            "Service: " + serviceQName + " is not defined in the WSDL file " + wsdlURI);
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
  public Wsdl(WSDLLocator wsdlLocator, QName serviceQName, String portName)
      throws AuthenticationException {
    this(wsdlLocator, serviceQName, portName, null, null);
  }

  public Wsdl(
      WSDLLocator wsdlLocator,
      QName serviceQName,
      String portName,
      String username,
      String password)
      throws AuthenticationException {

    // load and parse the WSDL
    try {
      wsdlDefinition = parse(wsdlLocator, username, password);
    } catch (AuthenticationException ae) {
      // throw it again or HopException will catch it
      throw ae;
    } catch (WSDLException | HopException e) {
      throw new RuntimeException(CONST_COULD_NOT_LOAD_WSDL_FILE + e.getMessage(), e);
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
   * @param username to use for authentication
   * @param password to use for authentication
   * @return wsdl Definition.
   * @throws WSDLException on error.
   */
  private Definition parse(WSDLLocator wsdlLocator, String username, String password)
      throws WSDLException, HopException, AuthenticationException {

    WSDLReader wsdlReader = getReader();
    try {

      return wsdlReader.readWSDL(wsdlLocator);
    } catch (WSDLException we) {
      readWsdl(wsdlReader, wsdlURI.toString(), username, password);
      return null;
    }
  }

  /**
   * Load and parse the WSDL file at the specified URI.
   *
   * @param wsdlURI URI of the WSDL file.
   * @param username to use for authentication
   * @param password to use for authentication
   * @return wsdl Definition
   * @throws WSDLException on error.
   */
  private Definition parse(URI wsdlURI, String username, String password)
      throws WSDLException, HopException, AuthenticationException {
    WSDLReader wsdlReader = getReader();
    return readWsdl(wsdlReader, wsdlURI.toString(), username, password);
  }

  private Definition readWsdl(WSDLReader wsdlReader, String uri, String username, String password)
      throws WSDLException, HopException, AuthenticationException {

    try {
      HttpProtocol http = new HttpProtocol();
      Document doc =
          XmlHandler.loadXmlString(http.get(wsdlURI.toString(), username, password), true, false);
      if (doc != null) {
        return (wsdlReader.readWSDL(doc.getBaseURI(), doc));
      } else {
        throw new HopException("Unable to get document.");
      }
    } catch (MalformedURLException mue) {
      throw new HopException(mue);
    } catch (AuthenticationException ae) {
      // re-throw this. If not IOException seems to catch it
      throw ae;
    } catch (IOException ioe) {
      throw new HopException(ioe);
    }
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
