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

package org.apache.hop.ui.core.metadata;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.GuiContextHandler;
import org.apache.hop.ui.hopgui.context.GuiContextUtil;
import org.apache.hop.ui.hopgui.perspective.metadata.MetadataPerspective;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;

/**
 * This is a utility class which allows you to create or edit metadata objects
 * in a generic fashion
 *
 * @author Matt
 * @since 2020-01-21
 */
public class MetadataManager<T extends IHopMetadata> {

	private IHopMetadataProvider metadataProvider;
	private IVariables variables;
	private ClassLoader classLoader;
	private Class<T> managedClass;

	public MetadataManager(IVariables variables, IHopMetadataProvider metadataProvider, Class<T> managedClass) {
		this.variables = variables;
		this.classLoader = managedClass.getClassLoader();
		this.metadataProvider = metadataProvider;
		this.managedClass = managedClass;
	}

	/**
	 * edit an element
	 *
	 * @return True if anything was changed
	 */
	public boolean edit() {
		HopGui hopGui = HopGui.getInstance();
		try {
			List<String> names = getNames();

			// Plugin details from the managed class...
			//
			HopMetadata hopMetadata = HopMetadataUtil.getHopMetadataAnnotation(managedClass);

			// Show an action dialog...
			//
			List<GuiAction> actions = new ArrayList<>();
			for (final String name : names) {
				GuiAction action = new GuiAction(name, GuiActionType.Modify, name,
						name + " : " + hopMetadata.description(), hopMetadata.iconImage(),
						(shiftAction, controlAction, t) -> editWithDialog(name));
				action.setClassLoader(getClassLoader());
				actions.add(action);
			}
			return GuiContextUtil.handleActionSelection(hopGui.getShell(),
					"Select the " + hopMetadata.name() + " to edit",
					new GuiContextHandler("HopGuiMetadataContext", actions));

		} catch (Exception e) {
			new ErrorDialog(hopGui.getShell(), "Error", "Error editing metadata", e);
			return false;
		}
	}

	/**
	 * delete an element
	 *
	 * @return True if anything was changed
	 */
	public boolean delete() {
		HopGui hopGui = HopGui.getInstance();
		try {
			List<String> names = getNames();

			HopMetadata hopMetadata = HopMetadataUtil.getHopMetadataAnnotation(managedClass);

			// Show an action dialog...
			//
			List<GuiAction> actions = new ArrayList<>();
			for (final String name : names) {
				GuiAction action = new GuiAction(name, GuiActionType.Delete, name,
						name + " : " + hopMetadata.description(), hopMetadata.iconImage(),
						(shiftAction, controlAction, t) -> delete(name));
				action.setClassLoader(getClassLoader());
				actions.add(action);
			}
			return GuiContextUtil.handleActionSelection(hopGui.getShell(),
					"Select the " + hopMetadata.name() + " to delete after confirmation",
					new GuiContextHandler("HopGuiMetadaContext", actions));

		} catch (Exception e) {
			new ErrorDialog(hopGui.getShell(), "Error", "Error deleting metadata", e);
			return false;
		}
	}

	/**
	 * We look at the managed class name, add Dialog to it and then simply us that
	 * class to edit the dialog.
	 *
	 * @param name The name of the element to edit
	 * @return True if anything was changed
	 */
	public boolean editWithDialog(String name) {

		if (StringUtils.isEmpty(name)) {
			return false;
		}

		HopGui hopGui = HopGui.getInstance();

		try {
			IHopMetadataSerializer<T> serializer = metadataProvider.getSerializer(managedClass);

			// Load the metadata element from the metadata
			//
			T element = serializer.load(name);
			if (element == null) {
				// Something removed or renamed the element in the background
				//
				throw new HopException("Unable to find element '" + name + "' in the metadata");
			}

			initializeElementVariables(element);

			MetadataEditor<T> editor = this.createEditor(element);
			editor.setTitle(getManagedName());

			MetadataEditorDialog dialog = new MetadataEditorDialog(hopGui.getShell(), editor);

			String result = dialog.open();
			if (result != null) {
				ExtensionPointHandler.callExtensionPoint(hopGui.getLog(),
						HopExtensionPoint.HopGuiMetadataObjectUpdated.id, element);
				return true;
			} else {
				return false;
			}
		} catch (Exception e) {
			new ErrorDialog(hopGui.getShell(), "Error", "Error editing metadata", e);
			return false;
		}
	}

	private void initializeElementVariables(T element) {
		if (element instanceof IVariables) {
			((IVariables) element).initializeVariablesFrom(variables);
		}
	}

	/**
	 * Delete an element
	 *
	 * @param name The name of the element to delete
	 * @return True if anything was deleted
	 */
	public boolean delete(String name) {
		return delete(name, false);
	}
	
	/**
	 * Delete an element
	 *
	 * @param name The name of the element to delete
	 * @param force Request confirmation before delete
	 * @return True if anything was deleted
	 */
	public boolean delete(String name, boolean force) {

		if (StringUtils.isEmpty(name)) {
			return false;
		}

		HopGui hopGui = HopGui.getInstance();

		if ( force==false ) {
		  MessageBox confirmBox = new MessageBox(HopGui.getInstance().getShell(), SWT.ICON_QUESTION | SWT.YES | SWT.NO);
		  confirmBox.setText("Delete?");
		  confirmBox.setMessage("Are you sure you want to delete element " + name + "?");
		  int anwser = confirmBox.open();
		  if ((anwser & SWT.YES) == 0) {
			return false;
		  }
		}
		
		try {
			IHopMetadataSerializer<T> serializer = getSerializer();

			// delete the metadata object from the metadata
			//
			T object = serializer.delete(name);

			// Just to be precise.
			//
			initializeElementVariables(object);

			ExtensionPointHandler.callExtensionPoint(hopGui.getLog(), HopExtensionPoint.HopGuiMetadataObjectDeleted.id,
					object);

			return true;

		} catch (Exception e) {
			new ErrorDialog(hopGui.getShell(), "Error", "Error deleting metadata element " + name, e);
			return false;
		}
	}

	public boolean rename(String oldName, String newName ) throws HopException {
	    IHopMetadataSerializer<T> serializer = this.getSerializer();
	          	   
	    if ( serializer.exists( newName ) ) {
	    	MessageBox messageBox = new MessageBox(HopGui.getInstance().getShell(), SWT.ICON_ERROR | SWT.OK );
			messageBox.setText(getManagedName());
			messageBox.setMessage("Name '"+newName+"' already existe.");
			messageBox.open();
			
			return false;
	    }

	    T metadata = this.load(oldName);
	    metadata.setName(newName);
	    this.save(metadata);	       
		this.delete(oldName, true);
			    
		return true;
	}
	
	public IHopMetadataSerializer<T> getSerializer() throws HopException {
		return metadataProvider.getSerializer(managedClass);
	}

	public void editWithEditor(String name) {
		if (name == null) {
			return;
		}

		HopGui hopGui = HopGui.getInstance();

		try {
			HopMetadata annotation = HopMetadataUtil.getHopMetadataAnnotation(managedClass);
		
			MetadataPerspective perspective = MetadataPerspective.getInstance();
			MetadataEditor<?> editor = perspective.findEditor(annotation.key(), name);

			if (editor == null) {

				// Load the metadata element from the metadata
				//
				IHopMetadataSerializer<T> serializer = metadataProvider.getSerializer(managedClass);
				T element = serializer.load(name);
				if (element == null) {
					// Something removed or renamed the element in the background
					//
					throw new HopException("Unable to find element '" + name + "' in the metadata");
				}

				initializeElementVariables(element);

				perspective.addEditor(createEditor(element));
			} else {
				perspective.setActiveEditor(editor);
			}
		} catch (Exception e) {
			new ErrorDialog(hopGui.getShell(), "Error", "Error editing metadata", e);
		}
	}

	public T newWithDialog() {
		HopGui hopGui = HopGui.getInstance();

		try {
			// Create a new instance of the managed class
			//
			T element = managedClass.newInstance();
			initializeElementVariables(element);

			ExtensionPointHandler.callExtensionPoint(hopGui.getLog(),
					HopExtensionPoint.HopGuiMetadataObjectCreateBeforeDialog.id, element);

			MetadataEditor<T> editor = this.createEditor(element);
			editor.setTitle(getManagedName());

			MetadataEditorDialog dialog = new MetadataEditorDialog(hopGui.getShell(), editor);

			String name = dialog.open();
			if (name != null) {
				ExtensionPointHandler.callExtensionPoint(hopGui.getLog(),
						HopExtensionPoint.HopGuiMetadataObjectCreated.id, element);
			}
			return element;
		} catch (Exception e) {
			new ErrorDialog(hopGui.getShell(), "Error", "Error creating new metadata element", e);
			return null;
		}
	}

	public T newWithEditor() {
		HopGui hopGui = HopGui.getInstance();

		try {

			// Create a new instance of the managed class
			//
			T element = managedClass.newInstance();
			initializeElementVariables(element);

			ExtensionPointHandler.callExtensionPoint(hopGui.getLog(),
					HopExtensionPoint.HopGuiMetadataObjectCreateBeforeDialog.id, element);

			MetadataEditor<T> editor = this.createEditor(element);
			editor.setTitle("New " + this.getManagedName());

			MetadataPerspective.getInstance().addEditor(editor);

			return element;
		} catch (Exception e) {
			new ErrorDialog(hopGui.getShell(), "Error", "Error creating new metadata element", e);
			return null;
		}
	}
	
	protected String getManagedName() {
		HopMetadata annotation = managedClass.getAnnotation(HopMetadata.class);
		if (annotation != null) {
			return annotation.name();
		}

		return null;
	}

	public List<String> getNames() throws HopException {
		try {
			List<String> names = getSerializer().listObjectNames();
			Collections.sort(names);
			return names;

		} catch (Exception e) {
			throw new HopException(
					"Unable to get list of element names in the MetaStore for class " + managedClass.getName(), e);
		}
	}

	public String[] getNamesArray() throws HopException {
		try {
			return getNames().toArray(new String[0]);
		} catch (Exception e) {
			throw new HopException(
					"Unable to get element names array in the MetaStore for class " + managedClass.getName(), e);
		}
	}

	protected MetadataEditor<T> createEditor(T metadata) throws HopException {

		// Find the class editor...
		//
		String className = managedClass.getName();
		className += "Editor";
		Class<MetadataEditor<T>> editorClass;
		try {
			editorClass = (Class<MetadataEditor<T>>) classLoader.loadClass(className);
		} catch (ClassNotFoundException e1) {
			className = managedClass.getName();
			className = className.replaceFirst("\\.hop\\.", ".hop.ui.");
			className += "Editor";
			try {
				editorClass = (Class<MetadataEditor<T>>) classLoader.loadClass(className);
			} catch (ClassNotFoundException e) {
				throw new HopException("Unable to find editor class (" + className + ")");
			}
		}

		// Create the class editor...
		// Always pass the HopGui, the metadata manager and the object to edit...
		//
		try {

			Class<?>[] constructorArguments = new Class<?>[] { HopGui.class, MetadataManager.class,
					metadata.getClass() };

			Constructor<MetadataEditor<T>> constructor;
			constructor = editorClass.getDeclaredConstructor(constructorArguments);

			if (constructor == null) {
				throw new HopException("Unable to find editor class (" + className
						+ ") constructor with arguments: HopGui, MetadataManager and IHopMetadata, T and optionally IVariables");
			}

			return constructor.newInstance(new Object[] { HopGui.getInstance(), this, metadata });
		} catch (InstantiationException | IllegalAccessException | NoSuchMethodException | SecurityException
				| IllegalArgumentException | InvocationTargetException e) {
			throw new HopException("Unable to create editor for class " + managedClass.getName(), e);
		}
	}

	public String calculateDialogClassname() {
		String dialogClassName = managedClass.getName();
		dialogClassName = dialogClassName.replaceFirst("\\.hop\\.", ".hop.ui.");
		dialogClassName += "Dialog";
		return dialogClassName;
	}

	public String calculateSimpleDialogClassname() {
		String dialogClassName = managedClass.getName();
		dialogClassName += "Dialog";
		return dialogClassName;
	}

	/**
	 * Gets metadataProvider
	 *
	 * @return value of metadataProvider
	 */
	public IHopMetadataProvider getMetadataProvider() {
		return metadataProvider;
	}

	/**
	 * @param metadataProvider The metadataProvider to set
	 */
	public void setMetadataProvider(IHopMetadataProvider metadataProvider) {
		this.metadataProvider = metadataProvider;
	}

	/**
	 * Gets space
	 *
	 * @return value of space
	 */
	public IVariables getVariables() {
		return variables;
	}

	/**
	 * @param variables The space to set
	 */
	public void setVariables(IVariables variables) {
		this.variables = variables;
	}

	/**
	 * Gets classLoader
	 *
	 * @return value of classLoader
	 */
	public ClassLoader getClassLoader() {
		return classLoader;
	}

	/**
	 * @param classLoader The classLoader to set
	 */
	public void setClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}

	/**
	 * Gets managedClass
	 *
	 * @return value of managedClass
	 */
	public Class<T> getManagedClass() {
		return managedClass;
	}

	/**
	 * @param managedClass The managedClass to set
	 */
	public void setManagedClass(Class<T> managedClass) {
		this.managedClass = managedClass;
	}

	public T load(String name) throws HopException {
		T element = getSerializer().load(name);
		initializeElementVariables(element);
		return element;
	}

	public void save(T metadata) throws HopException {
		getSerializer().save(metadata);
	}
}
