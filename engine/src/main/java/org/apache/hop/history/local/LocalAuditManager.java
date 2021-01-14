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
package org.apache.hop.history.local;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.history.AuditEvent;
import org.apache.hop.history.AuditList;
import org.apache.hop.history.AuditState;
import org.apache.hop.history.AuditStateMap;
import org.apache.hop.history.IAuditManager;

/**
 * The local audit manager stores its history in the hop home directory (~/.hop) under the history
 * folder So : $HOME/.hop/history
 *
 * <p>It will be done using a new metadata. - Event groups are mapped to namespaces -
 */
public class LocalAuditManager implements IAuditManager {

    private String rootFolder;

    public LocalAuditManager() {
        this.rootFolder = Const.HOP_AUDIT_FOLDER;
    }
    
    public LocalAuditManager(String rootFolder) {
      this.rootFolder = rootFolder;
    }

    @Override
    public void storeEvent(AuditEvent event) throws HopException {
        validateEvent(event);
        writeEvent(event);
    }

    private void writeEvent(AuditEvent event) throws HopException {
        String filename = calculateEventFilename(event);
        try {
            File file = new File(filename);
            File parentFolder = file.getParentFile();
            if (!parentFolder.exists()) {
                parentFolder.mkdirs();
            }

            // write the event to JSON...
            //
            ObjectMapper mapper = new ObjectMapper();
            mapper.writeValue(new File(filename), event);

            // TODO: clean up old events
            //
        } catch (IOException e) {
            throw new HopException("Unable to write event to filename '" + filename + "'", e);
        }
    }

    private String calculateEventFilename(AuditEvent event) {
        String typePath = calculateTypePath(event.getGroup(), event.getType());
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd-HHmmss.SSS");
        String filename = format.format(event.getDate()) + "-" + event.getOperation() + ".event";
        return typePath + File.separator + filename;
    }

    private String calculateGroupPath(String group) {
        return rootFolder + File.separator + group;
    }

    private String calculateTypePath(String group, String type) {
        return calculateGroupPath(group) + File.separator + type;
    }

    @Override
    public List<AuditEvent> findEvents(String group, String type, boolean unique)
            throws HopException {
        if (StringUtils.isEmpty(group)) {
            throw new HopException("You need to specify a group to find events");
        }
        if (StringUtils.isEmpty(type)) {
            throw new HopException("You need to specify a type to find events");
        }

        ArrayList<AuditEvent> events = new ArrayList<>();
        Map<String, AuditEvent> eventsMap = new HashMap<>();

        String folderPath = calculateTypePath(group, type);
        File folder = new File(folderPath);
        if (!folder.exists()) {
            return events;
        }

        // JSON to event mapper...
        //
        ObjectMapper mapper = new ObjectMapper();

        File[] eventFiles = folder.listFiles((dir, name) -> name.endsWith("event"));
        for (File eventFile : eventFiles) {
            try {
                AuditEvent event = mapper.readValue(eventFile, AuditEvent.class);
                if (unique) {
                    AuditEvent existing = eventsMap.get(event.getName());
                    if (existing == null || existing.getDate().compareTo(event.getDate()) < 0) {
                        eventsMap.put(event.getName(), event);
                    }
                } else {
                    events.add(event);
                }
            } catch (IOException e) {
                throw new HopException(
                        "Error reading event file '" + eventFile.toString() + "'", e);
            }
        }
        if (unique) {
            events.addAll(eventsMap.values());
        }

        // Sort the events by date descending (most recent first)
        //
        Collections.sort(events, Comparator.comparing(AuditEvent::getDate).reversed());

        return events;
    }

    @Override
    public void storeList(String group, String type, AuditList auditList) throws HopException {
        validateList(group, type, auditList);
        String filename = calculateGroupPath(group) + File.separator + type + ".list";
        checkFileAndFolder(filename);
        try {
            new ObjectMapper().writeValue(new File(filename), auditList);
        } catch (IOException e) {
            throw new HopException(
                    "It was not possible to write to audit list file '" + filename + "'", e);
        }
    }

    @Override
    public AuditList retrieveList(String group, String type) throws HopException {
        if (StringUtils.isEmpty(group)) {
            throw new HopException("You need a group before you can retrieve an audit list");
        }
        if (StringUtils.isEmpty(type)) {
            throw new HopException("To retrieve an audit list you need to specify the type");
        }

        String filename = calculateGroupPath(group) + File.separator + type + ".list";
        if (!checkFileAndFolder(filename)) {
            return new AuditList();
        } else {
            try {
                return new ObjectMapper().readValue(new File(filename), AuditList.class);
            } catch (IOException e) {
                throw new HopException(
                        "It was not possible to read audit list file '" + filename + "'", e);
            }
        }
    }

    @Override
    public void storeState(String group, String type, AuditState auditState) throws HopException {
        if (StringUtils.isEmpty(group)) {
            throw new HopException("The audit state needs a group to be able to store it");
        }
        if (StringUtils.isEmpty(type)) {
            throw new HopException("The audit state needs a type to be able to store it");
        }

        // Load the state map for the given group and type
        //
        AuditStateMap auditStateMap = loadAuditStateMap(group, type);

        // Add the state
        //
        auditStateMap.add(auditState);

        // save the state map
        //
        saveAuditStateMap(group, type, auditStateMap);
    }

    private String calculateStateFilename(String group, String type) {
        return calculateTypePath(group, type) + "-state.json";
    }

    /**
     * Check the file and folder. Create the parent folder if it doesn't exist.
     *
     * @param filename The filename to check
     * @return True if the file exists, false if it doesn't
     */
    private boolean checkFileAndFolder(String filename) {
        File file = new File(filename);
        if (!file.exists()) {
            // A new file, create the parent folder if needed
            //
            File parent = file.getParentFile();
            parent.mkdirs();
            return false;
        } else {
            return true;
        }
    }

    /**
     * Load the auditing state map for the specified group and type
     *
     * @param group
     * @param type
     * @return The map. An empty one if there are any loading problems. They are simply logged and
     *     you start over.
     */
    public AuditStateMap loadAuditStateMap(String group, String type) {
        String filename = calculateStateFilename(group, type);
        try {
            if (!checkFileAndFolder(filename)) {
                // A new file, create the parent folder if needed
                //
                return new AuditStateMap();
            } else {
                return new ObjectMapper().readValue(new File(filename), AuditStateMap.class);
            }
        } catch (Exception e) {
            LogChannel.GENERAL.logError("Error loading state map from file '" + filename + "'", e);
            return new AuditStateMap(); // probably corrupt: start over, it's not that important
        }
    }

    public void saveAuditStateMap(String group, String type, AuditStateMap auditStateMap)
            throws HopException {
        String filename = calculateStateFilename(group, type);
        try {
            checkFileAndFolder(filename);
            new ObjectMapper().writeValue(new File(filename), auditStateMap);
        } catch (Exception e) {
            throw new HopException("Error saving state map to file '" + filename + "'", e);
        }
    }

    @Override
    public AuditState retrieveState(String group, String type, String name) throws HopException {
        if (StringUtils.isEmpty(group)) {
            throw new HopException("To retrieve audit state you need to specify a group");
        }
        if (StringUtils.isEmpty(type)) {
            throw new HopException("To retrieve audit state you need to specify a type");
        }
        if (StringUtils.isEmpty(name)) {
            throw new HopException(
                    "Please specify the name of the object you which to retrieve the audit state for");
        }

        AuditStateMap auditStateMap = loadAuditStateMap(group, type);
        return auditStateMap.get(name);
    }

    private void validateEvent(AuditEvent event) throws HopException {
        if (StringUtils.isEmpty(event.getGroup())) {
            throw new HopException("Audit events need to belong to a group");
        }
        if (StringUtils.isEmpty(event.getType())) {
            throw new HopException("Audit events need to have a type");
        }
        if (StringUtils.isEmpty(event.getName())) {
            throw new HopException("Audit events need to have a name");
        }
        if (StringUtils.isEmpty(event.getOperation())) {
            throw new HopException("Audit events need to have an operation");
        }
        if (event.getDate() == null) {
            throw new HopException("Audit events need to have a date");
        }
    }

    private void validateList(String group, String type, AuditList auditList) throws HopException {
        if (StringUtils.isEmpty(group)) {
            throw new HopException("An audit list needs to belong to a group");
        }
        if (StringUtils.isEmpty(type)) {
            throw new HopException("An audit list needs to have a type");
        }
        if (auditList.getNames() == null) {
            throw new HopException("The audit list of names can't be null");
        }
    }

    private String calculateMapFilename(String group, String type) {
        return calculateTypePath(group, type) + "-map.json";
    }

    /**
     * Load the auditing state map for the specified group and type
     *
     * @param group
     * @param type
     * @return The map. An empty one if there are any loading problems. They are simply logged and
     *     you start over.
     */
    public Map<String, String> loadMap(String group, String type) {
        String filename = calculateMapFilename(group, type);
        try {
            if (!checkFileAndFolder(filename)) {
                // A new file, create the parent folder if needed
                //
                return new HashMap<>();
            } else {
                return new ObjectMapper().readValue(new File(filename), Map.class);
            }
        } catch (Exception e) {
            LogChannel.GENERAL.logError(
                    "Error loading strings map from file '" + filename + "'", e);
            return new HashMap<>(); // probably corrupt: start over, it's not that important
        }
    }

    public void saveMap(String group, String type, Map<String, String> map) throws HopException {
        String filename = calculateMapFilename(group, type);
        try {
            checkFileAndFolder(filename);
            new ObjectMapper().writeValue(new File(filename), map);
        } catch (Exception e) {
            throw new HopException("Error saving strings map to file '" + filename + "'", e);
        }
    }

    @Override
    public void clearEvents() throws HopException {
        try {
            FileUtils.deleteDirectory(Paths.get(rootFolder).toFile());
        } catch (IOException e) {
            throw new HopException(e);
        }
    }
}
