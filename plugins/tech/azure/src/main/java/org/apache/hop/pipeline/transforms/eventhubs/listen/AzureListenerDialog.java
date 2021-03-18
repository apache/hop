package org.apache.hop.pipeline.transforms.eventhubs.listen;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.HopPipelineFileType;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class AzureListenerDialog extends BaseTransformDialog implements ITransformDialog {

  private static Class<?> PKG =
      AzureListenerMeta.class; // for i18n purposes, needed by Translator2!!

  private Text wTransformName;
  private TextVar wNamespace;
  private TextVar wEventHub;
  private TextVar wSasKeyName;
  private TextVar wSasKey;
  private TextVar wBatchSize;
  private TextVar wPrefetchSize;
  private TextVar wOutputField;
  private TextVar wPartitionIdField;
  private TextVar wOffsetField;
  private TextVar wSequenceNumberField;
  private TextVar wHostField;
  private TextVar wEnqueuedTimeField;

  private TextVar wConsumerGroup;
  private TextVar wStorageConnectionString;
  private TextVar wStorageContainerName;

  private TextVar wBatchPipeline;
  private TextVar wBatchInput;
  private TextVar wBatchOutput;
  private TextVar wMaxWaitTime;

  private AzureListenerMeta input;

  public AzureListenerDialog(
      Shell parent,
      IVariables variables,
      Object inputMetadata,
      PipelineMeta transMeta,
      String transformName) {
    super(parent, variables, (BaseTransformMeta) inputMetadata, transMeta, transformName);
    input = (AzureListenerMeta) inputMetadata;
  }

  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText("Azure Event Hubs Listener");

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Transform name line
    //
    Label wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText("Transform name");
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(wlTransformName, 0, SWT.CENTER);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);
    Control lastControl = wTransformName;

    // Namespace
    //
    Label wlNamespace = new Label(shell, SWT.RIGHT);
    wlNamespace.setText("Event Hubs namespace");
    props.setLook(wlNamespace);
    FormData fdlNamespace = new FormData();
    fdlNamespace.left = new FormAttachment(0, 0);
    fdlNamespace.right = new FormAttachment(middle, -margin);
    fdlNamespace.top = new FormAttachment(lastControl, 2 * margin);
    wlNamespace.setLayoutData(fdlNamespace);
    wNamespace = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wNamespace);
    wNamespace.addModifyListener(lsMod);
    FormData fdNamespace = new FormData();
    fdNamespace.left = new FormAttachment(middle, 0);
    fdNamespace.right = new FormAttachment(100, 0);
    fdNamespace.top = new FormAttachment(wlNamespace, 0, SWT.CENTER);
    wNamespace.setLayoutData(fdNamespace);
    lastControl = wNamespace;

    Label wlEventHub = new Label(shell, SWT.RIGHT);
    wlEventHub.setText("Event Hubs Instance name");
    props.setLook(wlEventHub);
    FormData fdlEventHub = new FormData();
    fdlEventHub.left = new FormAttachment(0, 0);
    fdlEventHub.right = new FormAttachment(middle, -margin);
    fdlEventHub.top = new FormAttachment(lastControl, 2 * margin);
    wlEventHub.setLayoutData(fdlEventHub);
    wEventHub = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wEventHub);
    wEventHub.addModifyListener(lsMod);
    FormData fdEventHub = new FormData();
    fdEventHub.left = new FormAttachment(middle, 0);
    fdEventHub.right = new FormAttachment(100, 0);
    fdEventHub.top = new FormAttachment(wlEventHub, 0, SWT.CENTER);
    wEventHub.setLayoutData(fdEventHub);
    lastControl = wEventHub;

    Label wlSasKeyName = new Label(shell, SWT.RIGHT);
    wlSasKeyName.setText("SAS Policy key name");
    props.setLook(wlSasKeyName);
    FormData fdlSasKeyName = new FormData();
    fdlSasKeyName.left = new FormAttachment(0, 0);
    fdlSasKeyName.right = new FormAttachment(middle, -margin);
    fdlSasKeyName.top = new FormAttachment(lastControl, 2 * margin);
    wlSasKeyName.setLayoutData(fdlSasKeyName);
    wSasKeyName = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wSasKeyName);
    wSasKeyName.addModifyListener(lsMod);
    FormData fdSasKeyName = new FormData();
    fdSasKeyName.left = new FormAttachment(middle, 0);
    fdSasKeyName.right = new FormAttachment(100, 0);
    fdSasKeyName.top = new FormAttachment(wlSasKeyName, 0, SWT.CENTER);
    wSasKeyName.setLayoutData(fdSasKeyName);
    lastControl = wSasKeyName;

    Label wlSasKey = new Label(shell, SWT.RIGHT);
    wlSasKey.setText("SAS Key value");
    props.setLook(wlSasKey);
    FormData fdlSasKey = new FormData();
    fdlSasKey.left = new FormAttachment(0, 0);
    fdlSasKey.right = new FormAttachment(middle, -margin);
    fdlSasKey.top = new FormAttachment(lastControl, 2 * margin);
    wlSasKey.setLayoutData(fdlSasKey);
    wSasKey = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wSasKey.setEchoChar('*');
    props.setLook(wSasKey);
    wSasKey.addModifyListener(lsMod);
    FormData fdSasKey = new FormData();
    fdSasKey.left = new FormAttachment(middle, 0);
    fdSasKey.right = new FormAttachment(100, 0);
    fdSasKey.top = new FormAttachment(wlSasKey, 0, SWT.CENTER);
    wSasKey.setLayoutData(fdSasKey);
    lastControl = wSasKey;

    Label wlConsumerGroup = new Label(shell, SWT.RIGHT);
    wlConsumerGroup.setText("Consumer Group Name");
    props.setLook(wlConsumerGroup);
    FormData fdlConsumerGroup = new FormData();
    fdlConsumerGroup.left = new FormAttachment(0, 0);
    fdlConsumerGroup.right = new FormAttachment(middle, -margin);
    fdlConsumerGroup.top = new FormAttachment(lastControl, 2 * margin);
    wlConsumerGroup.setLayoutData(fdlConsumerGroup);
    wConsumerGroup = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wConsumerGroup);
    wConsumerGroup.addModifyListener(lsMod);
    FormData fdConsumerGroup = new FormData();
    fdConsumerGroup.left = new FormAttachment(middle, 0);
    fdConsumerGroup.right = new FormAttachment(100, 0);
    fdConsumerGroup.top = new FormAttachment(wlConsumerGroup, 0, SWT.CENTER);
    wConsumerGroup.setLayoutData(fdConsumerGroup);
    lastControl = wConsumerGroup;

    Label wlStorageContainerName = new Label(shell, SWT.RIGHT);
    wlStorageContainerName.setText("Storage Container name");
    props.setLook(wlStorageContainerName);
    FormData fdlStorageContainerName = new FormData();
    fdlStorageContainerName.left = new FormAttachment(0, 0);
    fdlStorageContainerName.right = new FormAttachment(middle, -margin);
    fdlStorageContainerName.top = new FormAttachment(lastControl, 2 * margin);
    wlStorageContainerName.setLayoutData(fdlStorageContainerName);
    wStorageContainerName = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wStorageContainerName);
    wStorageContainerName.addModifyListener(lsMod);
    FormData fdStorageContainerName = new FormData();
    fdStorageContainerName.left = new FormAttachment(middle, 0);
    fdStorageContainerName.right = new FormAttachment(100, 0);
    fdStorageContainerName.top = new FormAttachment(wlStorageContainerName, 0, SWT.CENTER);
    wStorageContainerName.setLayoutData(fdStorageContainerName);
    lastControl = wStorageContainerName;

    Label wlStorageConnectionString = new Label(shell, SWT.RIGHT);
    wlStorageConnectionString.setText("Storage Connection String");
    props.setLook(wlStorageConnectionString);
    FormData fdlStorageConnectionString = new FormData();
    fdlStorageConnectionString.left = new FormAttachment(0, 0);
    fdlStorageConnectionString.right = new FormAttachment(middle, -margin);
    fdlStorageConnectionString.top = new FormAttachment(lastControl, 2 * margin);
    wlStorageConnectionString.setLayoutData(fdlStorageConnectionString);
    wStorageConnectionString = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wStorageConnectionString.setEchoChar('*');
    props.setLook(wStorageConnectionString);
    wStorageConnectionString.addModifyListener(lsMod);
    FormData fdStorageConnectionString = new FormData();
    fdStorageConnectionString.left = new FormAttachment(middle, 0);
    fdStorageConnectionString.right = new FormAttachment(100, 0);
    fdStorageConnectionString.top = new FormAttachment(wlStorageConnectionString, 0, SWT.CENTER);
    wStorageConnectionString.setLayoutData(fdStorageConnectionString);
    lastControl = wStorageConnectionString;

    Label wlBatchSize = new Label(shell, SWT.RIGHT);
    wlBatchSize.setText("Batch size");
    props.setLook(wlBatchSize);
    FormData fdlBatchSize = new FormData();
    fdlBatchSize.left = new FormAttachment(0, 0);
    fdlBatchSize.right = new FormAttachment(middle, -margin);
    fdlBatchSize.top = new FormAttachment(lastControl, 2 * margin);
    wlBatchSize.setLayoutData(fdlBatchSize);
    wBatchSize = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wBatchSize);
    wBatchSize.addModifyListener(lsMod);
    FormData fdBatchSize = new FormData();
    fdBatchSize.left = new FormAttachment(middle, 0);
    fdBatchSize.right = new FormAttachment(100, 0);
    fdBatchSize.top = new FormAttachment(wlBatchSize, 0, SWT.CENTER);
    wBatchSize.setLayoutData(fdBatchSize);
    lastControl = wBatchSize;

    Label wlPrefetchSize = new Label(shell, SWT.RIGHT);
    wlPrefetchSize.setText("Prefetch size");
    props.setLook(wlPrefetchSize);
    FormData fdlPrefetchSize = new FormData();
    fdlPrefetchSize.left = new FormAttachment(0, 0);
    fdlPrefetchSize.right = new FormAttachment(middle, -margin);
    fdlPrefetchSize.top = new FormAttachment(lastControl, 2 * margin);
    wlPrefetchSize.setLayoutData(fdlPrefetchSize);
    wPrefetchSize = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wPrefetchSize);
    wPrefetchSize.addModifyListener(lsMod);
    FormData fdPrefetchSize = new FormData();
    fdPrefetchSize.left = new FormAttachment(middle, 0);
    fdPrefetchSize.right = new FormAttachment(100, 0);
    fdPrefetchSize.top = new FormAttachment(wlPrefetchSize, 0, SWT.CENTER);
    wPrefetchSize.setLayoutData(fdPrefetchSize);
    lastControl = wPrefetchSize;

    Label wlOutputField = new Label(shell, SWT.RIGHT);
    wlOutputField.setText("Message (data) output field name");
    props.setLook(wlOutputField);
    FormData fdlOutputField = new FormData();
    fdlOutputField.left = new FormAttachment(0, 0);
    fdlOutputField.right = new FormAttachment(middle, -margin);
    fdlOutputField.top = new FormAttachment(lastControl, 2 * margin);
    wlOutputField.setLayoutData(fdlOutputField);
    wOutputField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wOutputField);
    wOutputField.addModifyListener(lsMod);
    FormData fdOutputField = new FormData();
    fdOutputField.left = new FormAttachment(middle, 0);
    fdOutputField.right = new FormAttachment(100, 0);
    fdOutputField.top = new FormAttachment(wlOutputField, 0, SWT.CENTER);
    wOutputField.setLayoutData(fdOutputField);
    lastControl = wOutputField;

    Label wlPartitionIdField = new Label(shell, SWT.RIGHT);
    wlPartitionIdField.setText("Partition ID field name");
    props.setLook(wlPartitionIdField);
    FormData fdlPartitionIdField = new FormData();
    fdlPartitionIdField.left = new FormAttachment(0, 0);
    fdlPartitionIdField.right = new FormAttachment(middle, -margin);
    fdlPartitionIdField.top = new FormAttachment(lastControl, 2 * margin);
    wlPartitionIdField.setLayoutData(fdlPartitionIdField);
    wPartitionIdField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wPartitionIdField);
    wPartitionIdField.addModifyListener(lsMod);
    FormData fdPartitionIdField = new FormData();
    fdPartitionIdField.left = new FormAttachment(middle, 0);
    fdPartitionIdField.right = new FormAttachment(100, 0);
    fdPartitionIdField.top = new FormAttachment(wlPartitionIdField, 0, SWT.CENTER);
    wPartitionIdField.setLayoutData(fdPartitionIdField);
    lastControl = wPartitionIdField;

    Label wlOffsetField = new Label(shell, SWT.RIGHT);
    wlOffsetField.setText("Offset field name");
    props.setLook(wlOffsetField);
    FormData fdlOffsetField = new FormData();
    fdlOffsetField.left = new FormAttachment(0, 0);
    fdlOffsetField.right = new FormAttachment(middle, -margin);
    fdlOffsetField.top = new FormAttachment(lastControl, 2 * margin);
    wlOffsetField.setLayoutData(fdlOffsetField);
    wOffsetField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wOffsetField);
    wOffsetField.addModifyListener(lsMod);
    FormData fdOffsetField = new FormData();
    fdOffsetField.left = new FormAttachment(middle, 0);
    fdOffsetField.right = new FormAttachment(100, 0);
    fdOffsetField.top = new FormAttachment(wlOffsetField, 0, SWT.CENTER);
    wOffsetField.setLayoutData(fdOffsetField);
    lastControl = wOffsetField;

    Label wlSequenceNumberField = new Label(shell, SWT.RIGHT);
    wlSequenceNumberField.setText("Sequence number field name");
    props.setLook(wlSequenceNumberField);
    FormData fdlSequenceNumberField = new FormData();
    fdlSequenceNumberField.left = new FormAttachment(0, 0);
    fdlSequenceNumberField.right = new FormAttachment(middle, -margin);
    fdlSequenceNumberField.top = new FormAttachment(lastControl, 2 * margin);
    wlSequenceNumberField.setLayoutData(fdlSequenceNumberField);
    wSequenceNumberField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wSequenceNumberField);
    wSequenceNumberField.addModifyListener(lsMod);
    FormData fdSequenceNumberField = new FormData();
    fdSequenceNumberField.left = new FormAttachment(middle, 0);
    fdSequenceNumberField.right = new FormAttachment(100, 0);
    fdSequenceNumberField.top = new FormAttachment(wlSequenceNumberField, 0, SWT.CENTER);
    wSequenceNumberField.setLayoutData(fdSequenceNumberField);
    lastControl = wSequenceNumberField;

    Label wlHostField = new Label(shell, SWT.RIGHT);
    wlHostField.setText("Host (owner) field name");
    props.setLook(wlHostField);
    FormData fdlHostField = new FormData();
    fdlHostField.left = new FormAttachment(0, 0);
    fdlHostField.right = new FormAttachment(middle, -margin);
    fdlHostField.top = new FormAttachment(lastControl, 2 * margin);
    wlHostField.setLayoutData(fdlHostField);
    wHostField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wHostField);
    wHostField.addModifyListener(lsMod);
    FormData fdHostField = new FormData();
    fdHostField.left = new FormAttachment(middle, 0);
    fdHostField.right = new FormAttachment(100, 0);
    fdHostField.top = new FormAttachment(wlHostField, 0, SWT.CENTER);
    wHostField.setLayoutData(fdHostField);
    lastControl = wHostField;

    Label wlEnqueuedTimeField = new Label(shell, SWT.RIGHT);
    wlEnqueuedTimeField.setText("Enqueued time field name");
    props.setLook(wlEnqueuedTimeField);
    FormData fdlEnqueuedTimeField = new FormData();
    fdlEnqueuedTimeField.left = new FormAttachment(0, 0);
    fdlEnqueuedTimeField.right = new FormAttachment(middle, -margin);
    fdlEnqueuedTimeField.top = new FormAttachment(lastControl, 2 * margin);
    wlEnqueuedTimeField.setLayoutData(fdlEnqueuedTimeField);
    wEnqueuedTimeField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wEnqueuedTimeField);
    wEnqueuedTimeField.addModifyListener(lsMod);
    FormData fdEnqueuedTimeField = new FormData();
    fdEnqueuedTimeField.left = new FormAttachment(middle, 0);
    fdEnqueuedTimeField.right = new FormAttachment(100, 0);
    fdEnqueuedTimeField.top = new FormAttachment(wlEnqueuedTimeField, 0, SWT.CENTER);
    wEnqueuedTimeField.setLayoutData(fdEnqueuedTimeField);
    lastControl = wEnqueuedTimeField;

    Label wlSeparator1 = new Label(shell, SWT.SEPARATOR | SWT.HORIZONTAL);
    props.setLook(wlSeparator1);
    FormData fdlSeparator1 = new FormData();
    fdlSeparator1.left = new FormAttachment(0, margin);
    fdlSeparator1.right = new FormAttachment(100, -margin);
    fdlSeparator1.top = new FormAttachment(lastControl, 4 * margin);
    wlSeparator1.setLayoutData(fdlSeparator1);
    lastControl = wlSeparator1;

    Label wlBatchPipeline = new Label(shell, SWT.RIGHT);
    wlBatchPipeline.setText("Batch pipeline");
    props.setLook(wlBatchPipeline);
    FormData fdlBatchPipeline = new FormData();
    fdlBatchPipeline.left = new FormAttachment(0, 0);
    fdlBatchPipeline.right = new FormAttachment(middle, -margin);
    fdlBatchPipeline.top = new FormAttachment(lastControl, 4 * margin);
    wlBatchPipeline.setLayoutData(fdlBatchPipeline);

    Button wbBatchPipeline = new Button(shell, SWT.PUSH);
    props.setLook(wbBatchPipeline);
    wbBatchPipeline.setText(BaseMessages.getString("System.Button.Browse"));
    wbBatchPipeline.addListener(SWT.Selection, e -> browseForPipeline());
    FormData fdbBatchPipeline = new FormData();
    fdbBatchPipeline.right = new FormAttachment(100, -margin);
    fdbBatchPipeline.top = new FormAttachment(wlBatchPipeline, 0, SWT.CENTER);
    wbBatchPipeline.setLayoutData(fdbBatchPipeline);
    wBatchPipeline = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook( wBatchPipeline );
    wBatchPipeline.addModifyListener(lsMod);
    FormData fdBatchPipeline = new FormData();
    fdBatchPipeline.left = new FormAttachment(middle, 0);
    fdBatchPipeline.right = new FormAttachment(wbBatchPipeline, -margin );
    fdBatchPipeline.top = new FormAttachment(wlBatchPipeline, 0, SWT.CENTER);
    wBatchPipeline.setLayoutData(fdBatchPipeline);
    lastControl = wBatchPipeline;

    Label wlBatchInput = new Label(shell, SWT.RIGHT);
    wlBatchInput.setText("Pipeline input transform");
    props.setLook(wlBatchInput);
    FormData fdlBatchInput = new FormData();
    fdlBatchInput.left = new FormAttachment(0, 0);
    fdlBatchInput.right = new FormAttachment(middle, -margin);
    fdlBatchInput.top = new FormAttachment(lastControl, 2 * margin);
    wlBatchInput.setLayoutData(fdlBatchInput);
    Button wbBatchInput = new Button(shell, SWT.PUSH);
    props.setLook(wbBatchInput);
    wbBatchInput.setText("Select...");
    wbBatchInput.addListener(SWT.Selection, e -> selectInputTransform());
    FormData fdbBatchInput = new FormData();
    fdbBatchInput.right = new FormAttachment(100, -margin);
    fdbBatchInput.top = new FormAttachment(wlBatchInput, 0, SWT.CENTER);
    wbBatchInput.setLayoutData(fdbBatchInput);
    wBatchInput = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wBatchInput);
    wBatchInput.addModifyListener(lsMod);
    FormData fdBatchInput = new FormData();
    fdBatchInput.left = new FormAttachment(middle, 0);
    fdBatchInput.right = new FormAttachment(wbBatchInput, -margin);
    fdBatchInput.top = new FormAttachment(wlBatchInput, 0, SWT.CENTER);
    wBatchInput.setLayoutData(fdBatchInput);
    lastControl = wBatchInput;

    Label wlBatchOutput = new Label(shell, SWT.RIGHT);
    wlBatchOutput.setText("Pipeline output transform");
    props.setLook(wlBatchOutput);
    FormData fdlBatchOutput = new FormData();
    fdlBatchOutput.left = new FormAttachment(0, 0);
    fdlBatchOutput.right = new FormAttachment(middle, -margin);
    fdlBatchOutput.top = new FormAttachment(lastControl, 2 * margin);
    wlBatchOutput.setLayoutData(fdlBatchOutput);
    Button wbBatchOutput = new Button(shell, SWT.PUSH);
    props.setLook(wbBatchOutput);
    wbBatchOutput.setText("Select...");
    wbBatchOutput.addListener(SWT.Selection, e -> selectOutputTransform());
    FormData fdbBatchOutput = new FormData();
    fdbBatchOutput.right = new FormAttachment(100, -margin);
    fdbBatchOutput.top = new FormAttachment(wlBatchOutput, 0, SWT.CENTER);
    wbBatchOutput.setLayoutData(fdbBatchOutput);
    wBatchOutput = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wBatchOutput);
    wBatchOutput.addModifyListener(lsMod);
    FormData fdBatchOutput = new FormData();
    fdBatchOutput.left = new FormAttachment(middle, 0);
    fdBatchOutput.right = new FormAttachment(wbBatchOutput, -margin);
    fdBatchOutput.top = new FormAttachment(wlBatchOutput, 0, SWT.CENTER);
    wBatchOutput.setLayoutData(fdBatchOutput);
    lastControl = wBatchOutput;

    Label wlMaxWaitTime = new Label(shell, SWT.RIGHT);
    wlMaxWaitTime.setText("Maximum wait time (ms)");
    props.setLook(wlMaxWaitTime);
    FormData fdlMaxWaitTime = new FormData();
    fdlMaxWaitTime.left = new FormAttachment(0, 0);
    fdlMaxWaitTime.right = new FormAttachment(middle, -margin);
    fdlMaxWaitTime.top = new FormAttachment(lastControl, 2 * margin);
    wlMaxWaitTime.setLayoutData(fdlMaxWaitTime);
    wMaxWaitTime = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wMaxWaitTime);
    wMaxWaitTime.addModifyListener(lsMod);
    FormData fdMaxWaitTime = new FormData();
    fdMaxWaitTime.left = new FormAttachment(middle, 0);
    fdMaxWaitTime.right = new FormAttachment(100, 0);
    fdMaxWaitTime.top = new FormAttachment(wlMaxWaitTime, 0, SWT.CENTER);
    wMaxWaitTime.setLayoutData(fdMaxWaitTime);
    lastControl = wMaxWaitTime;

    // Position the buttons at the bottom of the dialog.
    //
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wCancel}, margin, lastControl);

    lsDef =
        new SelectionAdapter() {
          public void widgetDefaultSelected(SelectionEvent e) {
            ok();
          }
        };

    wTransformName.addSelectionListener(lsDef);
    wNamespace.addSelectionListener(lsDef);
    wEventHub.addSelectionListener(lsDef);
    wSasKeyName.addSelectionListener(lsDef);
    wSasKey.addSelectionListener(lsDef);
    wBatchSize.addSelectionListener(lsDef);
    wPrefetchSize.addSelectionListener(lsDef);
    wConsumerGroup.addSelectionListener(lsDef);
    wStorageConnectionString.addSelectionListener(lsDef);
    wStorageContainerName.addSelectionListener(lsDef);

    wOutputField.addSelectionListener(lsDef);
    wPartitionIdField.addSelectionListener(lsDef);
    wOffsetField.addSelectionListener(lsDef);
    wSequenceNumberField.addSelectionListener(lsDef);
    wHostField.addSelectionListener(lsDef);
    wEnqueuedTimeField.addSelectionListener(lsDef);

    wBatchPipeline.addSelectionListener(lsDef);
    wBatchInput.addSelectionListener(lsDef);
    wBatchOutput.addSelectionListener(lsDef);
    wMaxWaitTime.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    input.setChanged(changed);

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void browseForPipeline() {
    HopPipelineFileType<PipelineMeta> type =
        HopGui.getDataOrchestrationPerspective().getPipelineFileType();

    BaseDialog.presentFileDialog(
        shell,
        wBatchPipeline,
        (FileObject) null,
        type.getFilterExtensions(),
        type.getFilterNames(),
        true);
  }

  private void selectInputTransform() {
    selectTransform( wBatchInput, "Select the transform to send the messages to:" );
  }

  private void selectOutputTransform() {
    selectTransform( wBatchOutput, "Select a transform to read output from:" );
  }


  private void selectTransform(TextVar textVar, String message) {
    try {
      AzureListenerMeta meta = new AzureListenerMeta();
      getInfo(meta);
      PipelineMeta pipelineMeta =
          AzureListenerMeta.loadBatchPipelineMeta(meta, metadataProvider, variables);
      String[] transformNames = pipelineMeta.getTransformNames();
      EnterSelectionDialog dialog = new EnterSelectionDialog( shell, transformNames, "Select transform", message );
      String transformName = dialog.open();
      if (transformName!=null) {
        textVar.setText( transformName );
      }
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error selecting transform", e);
    }
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  public void getData() {
    wTransformName.setText(Const.NVL(transformName, ""));
    wNamespace.setText(Const.NVL(input.getNamespace(), ""));
    wEventHub.setText(Const.NVL(input.getEventHubName(), ""));
    wSasKeyName.setText(Const.NVL(input.getSasKeyName(), ""));
    wSasKey.setText(Const.NVL(input.getSasKey(), ""));
    wConsumerGroup.setText(Const.NVL(input.getConsumerGroupName(), ""));
    wStorageContainerName.setText(Const.NVL(input.getStorageContainerName(), ""));
    wStorageConnectionString.setText(Const.NVL(input.getStorageConnectionString(), ""));
    wBatchSize.setText(Const.NVL(input.getBatchSize(), ""));
    wPrefetchSize.setText(Const.NVL(input.getPrefetchSize(), ""));
    wOutputField.setText(Const.NVL(input.getOutputField(), ""));
    wPartitionIdField.setText(Const.NVL(input.getPartitionIdField(), ""));
    wOffsetField.setText(Const.NVL(input.getOffsetField(), ""));
    wHostField.setText(Const.NVL(input.getHostField(), ""));
    wSequenceNumberField.setText(Const.NVL(input.getSequenceNumberField(), ""));
    wEnqueuedTimeField.setText(Const.NVL(input.getEnqueuedTimeField(), ""));

    wBatchPipeline.setText(Const.NVL(input.getBatchPipeline(), ""));
    wBatchInput.setText(Const.NVL(input.getBatchInputTransform(), ""));
    wBatchOutput.setText(Const.NVL(input.getBatchOutputTransform(), ""));
    wMaxWaitTime.setText(Const.NVL(input.getBatchMaxWaitTime(), ""));
  }

  private void ok() {
    if (StringUtils.isEmpty(wTransformName.getText())) {
      return;
    }
    transformName = wTransformName.getText(); // return value
    getInfo(input);

    dispose();
  }

  private void getInfo(AzureListenerMeta meta) {

    meta.setNamespace(wNamespace.getText());
    meta.setEventHubName(wEventHub.getText());
    meta.setSasKeyName(wSasKeyName.getText());
    meta.setSasKey(wSasKey.getText());
    meta.setConsumerGroupName(wConsumerGroup.getText());
    meta.setStorageContainerName(wStorageContainerName.getText());
    meta.setStorageConnectionString(wStorageConnectionString.getText());
    meta.setBatchSize(wBatchSize.getText());
    meta.setPrefetchSize(wPrefetchSize.getText());
    meta.setOutputField(wOutputField.getText());
    meta.setPartitionIdField(wPartitionIdField.getText());
    meta.setOffsetField(wOffsetField.getText());
    meta.setSequenceNumberField(wSequenceNumberField.getText());
    meta.setHostField(wHostField.getText());
    meta.setEnqueuedTimeField(wEnqueuedTimeField.getText());

    meta.setBatchPipeline( wBatchPipeline.getText());
    meta.setBatchInputTransform(wBatchInput.getText());
    meta.setBatchOutputTransform(wBatchOutput.getText());
    meta.setBatchMaxWaitTime(wMaxWaitTime.getText());
  }
}
