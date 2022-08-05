package org.apache.hop.arrow.transforms.arrowdecode;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaArrowVector;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.*;
import java.util.List;

public class ArrowDecodeDialog extends BaseTransformDialog implements ITransformDialog {
    private static final Class<?> PKG = ArrowDecodeMeta.class; // For Translator

    private ArrowDecodeMeta input;

    private Combo wSourceField;
    private TableView wFields;
    private RowProducer rowProducer;

    public ArrowDecodeDialog(
            Shell parent,
            IVariables variables,
            Object baseTransformMeta,
            PipelineMeta pipelineMeta,
            String transformName) {
        super(parent, variables, (BaseTransformMeta) baseTransformMeta, pipelineMeta, transformName);

        input = (ArrowDecodeMeta) baseTransformMeta;
    }

    @Override
    public String open() {

        Shell parent = getParent();

        shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
        props.setLook(shell);
        setShellImage(shell, input);

        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = Const.FORM_MARGIN;
        formLayout.marginHeight = Const.FORM_MARGIN;

        shell.setLayout(formLayout);
        shell.setText(BaseMessages.getString(PKG, "ArrowDecodeDialog.Shell.Title"));

        int middle = props.getMiddlePct();
        int margin = props.getMargin();

        // Some buttons at the bottom
        wOk = new Button(shell, SWT.PUSH);
        wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
        wOk.addListener(SWT.Selection, e -> ok());
        wGet = new Button(shell, SWT.PUSH);
        wGet.setText(BaseMessages.getString(PKG, "System.Button.GetFields"));
        wGet.addListener(SWT.Selection, e -> getFields());
        wCancel = new Button(shell, SWT.PUSH);
        wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
        wCancel.addListener(SWT.Selection, e -> cancel());
        setButtonPositions(new Button[] {wOk, wGet, wCancel}, margin, null);

        // TransformName line
        wlTransformName = new Label(shell, SWT.RIGHT);
        wlTransformName.setText(BaseMessages.getString(PKG, "ArrowDecodeDialog.TransformName.Label"));
        props.setLook(wlTransformName);
        fdlTransformName = new FormData();
        fdlTransformName.left = new FormAttachment(0, 0);
        fdlTransformName.right = new FormAttachment(middle, -margin);
        fdlTransformName.top = new FormAttachment(0, margin);
        wlTransformName.setLayoutData(fdlTransformName);
        wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        wTransformName.setText(transformName);
        props.setLook(wTransformName);
        fdTransformName = new FormData();
        fdTransformName.left = new FormAttachment(middle, 0);
        fdTransformName.top = new FormAttachment(wlTransformName, 0, SWT.CENTER);
        fdTransformName.right = new FormAttachment(100, 0);
        wTransformName.setLayoutData(fdTransformName);
        Control lastControl = wTransformName;

        Label wlSourceField = new Label(shell, SWT.RIGHT);
        wlSourceField.setText(BaseMessages.getString(PKG, "ArrowDecodeDialog.SourceField.Label"));
        props.setLook(wlSourceField);
        FormData fdlSourceField = new FormData();
        fdlSourceField.left = new FormAttachment(0, 0);
        fdlSourceField.right = new FormAttachment(middle, -margin);
        fdlSourceField.top = new FormAttachment(lastControl, margin * 2);
        wlSourceField.setLayoutData(fdlSourceField);
        wSourceField = new Combo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
        wSourceField.setText(transformName);
        props.setLook(wSourceField);
        FormData fdSourceField = new FormData();
        fdSourceField.left = new FormAttachment(middle, 0);
        fdSourceField.top = new FormAttachment(wlSourceField, 0, SWT.CENTER);
        fdSourceField.right = new FormAttachment(100, 0);
        wSourceField.setLayoutData(fdSourceField);
        lastControl = wSourceField;

        Label wlFields = new Label(shell, SWT.LEFT);
        wlFields.setText(BaseMessages.getString(PKG, "ArrowDecodeDialog.Fields.Label"));
        props.setLook(wlFields);
        FormData fdlFields = new FormData();
        fdlFields.left = new FormAttachment(0, 0);
        fdlFields.top = new FormAttachment(lastControl, margin);
        wlFields.setLayoutData(fdlFields);

        ColumnInfo[] fieldsColumns =
                new ColumnInfo[] {
                        new ColumnInfo(
                                BaseMessages.getString(PKG, "ArrowDecodeDialog.Fields.Column.SourceField"),
                                ColumnInfo.COLUMN_TYPE_TEXT,
                                false,
                                false),
                        new ColumnInfo(
                                BaseMessages.getString(PKG, "ArrowDecodeDialog.Fields.Column.SourceType"),
                                ColumnInfo.COLUMN_TYPE_CCOMBO,
                                new String[] {
                                        "String", "Int", "Long", "Float", "Double", "Boolean", "Bytes", "Null", "Record",
                                        "Enum", "Array", "Map", "Union", "Fixed"
                                },
                                false),
                        new ColumnInfo(
                                BaseMessages.getString(PKG, "ArrowDecodeDialog.Fields.Column.TargetField"),
                                ColumnInfo.COLUMN_TYPE_TEXT,
                                false,
                                false),
                        new ColumnInfo(
                                BaseMessages.getString(PKG, "ArrowDecodeDialog.Fields.Column.TargetType"),
                                ColumnInfo.COLUMN_TYPE_CCOMBO,
                                ValueMetaFactory.getValueMetaNames(),
                                false),
                        new ColumnInfo(
                                BaseMessages.getString(PKG, "ArrowDecodeDialog.Fields.Column.TargetFormat"),
                                ColumnInfo.COLUMN_TYPE_CCOMBO,
                                Const.getConversionFormats(),
                                false),
                        new ColumnInfo(
                                BaseMessages.getString(PKG, "ArrowDecodeDialog.Fields.Column.TargetLength"),
                                ColumnInfo.COLUMN_TYPE_TEXT,
                                false,
                                false),
                        new ColumnInfo(
                                BaseMessages.getString(PKG, "ArrowDecodeDialog.Fields.Column.TargetPrecision"),
                                ColumnInfo.COLUMN_TYPE_TEXT,
                                false,
                                false)
                };

        wFields =
                new TableView(
                        variables,
                        shell,
                        SWT.NONE,
                        fieldsColumns,
                        input.getTargetFields().size(),
                        false,
                        null,
                        props);
        props.setLook(wFields);
        FormData fdFields = new FormData();
        fdFields.left = new FormAttachment(0, 0);
        fdFields.top = new FormAttachment(wlFields, margin);
        fdFields.right = new FormAttachment(100, 0);
        fdFields.bottom = new FormAttachment(wOk, -2 * margin);
        wFields.setLayoutData(fdFields);

        getData();

        BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

        return transformName;
    }

    /** Copy information from the meta-data input to the dialog fields. */
    public void getData() {

        try {
            // Get the fields from the previous transforms:
            wSourceField.setItems(
                    pipelineMeta.getPrevTransformFields(variables, transformMeta).getFieldNames());
        } catch (Exception e) {
            // Ignore exception
        }
        wSourceField.setText(Const.NVL(input.getSourceFieldName(), ""));

        int rowNr = 0;
        for (TargetField targetField : input.getTargetFields()) {
            TableItem item = wFields.table.getItem(rowNr++);
            int col = 1;
            item.setText(col++, Const.NVL(targetField.getSourceField(), ""));
            item.setText(col++, Const.NVL(targetField.getTargetFieldName(), ""));
            item.setText(col++, Const.NVL(targetField.getTargetType(), ""));
            item.setText(col++, Const.NVL(targetField.getTargetFormat(), ""));
            item.setText(col++, Const.NVL(targetField.getTargetLength(), ""));
            item.setText(col++, Const.NVL(targetField.getTargetPrecision(), ""));
        }

        wTransformName.selectAll();
        wTransformName.setFocus();
    }

    private void cancel() {
        transformName = null;
        dispose();
    }

    private void ok() {
        if (Utils.isEmpty(wTransformName.getText())) {
            return;
        }

        input.setSourceFieldName(wSourceField.getText());
        input.getTargetFields().clear();
        for (TableItem item : wFields.getNonEmptyItems()) {
            int col = 1;
            String sourceField = item.getText(col++);
            String targetField = item.getText(col++);
            String targetType = item.getText(col++);
            String targetFormat = item.getText(col++);
            String targetLength = item.getText(col++);
            String targetPrecision = item.getText(col);
            input
                    .getTargetFields()
                    .add(
                            new TargetField(
                                    sourceField,
                                    targetField,
                                    targetType,
                                    targetFormat,
                                    targetLength,
                                    targetPrecision));
        }

        transformName = wTransformName.getText(); // return value
        transformMeta.setChanged();

        dispose();
    }

    private void getFields() {
        try {

            Map<String, Field> fieldsMap = new HashMap<>();

            // If we have a source field name we can see if it's an Arrow type with a schema...
            //
            String fieldName = wSourceField.getText();
            if (StringUtils.isNotEmpty(fieldName)) {
                IRowMeta fields = pipelineMeta.getPrevTransformFields(variables, transformName);
                IValueMeta valueMeta = fields.searchValueMeta(fieldName);
                if (valueMeta != null && valueMeta.getType() == IValueMeta.TYPE_ARROW) {
                    Schema schema = ((ValueMetaArrowVector) valueMeta).getSchema();
                    if (schema != null) {
                        for (Field field : schema.getFields()) {
                            fieldsMap.put(field.getName(), field);
                        }
                    }
                }
            }

            if (fieldsMap.isEmpty()) {
                // Sorry, we can't do anything...
                return;
            }

            List<String> names = new ArrayList<>(fieldsMap.keySet());
            names.sort(Comparator.comparing(String::toLowerCase));
            for (String name : names) {
                Field field = fieldsMap.get(name);
                String typeDesc = StringUtil.initCap(field.getFieldType().toString());
                int hopType = ArrowDecode.getStandardHopType(field);
                String hopTypeDesc = ValueMetaFactory.getValueMetaName(hopType);

                TableItem item = new TableItem(wFields.table, SWT.NONE);
                item.setText(1, Const.NVL(field.getName(), ""));
                item.setText(2, typeDesc);
                item.setText(3, Const.NVL(field.getName(), ""));
                item.setText(4, hopTypeDesc);
            }
            wFields.optimizeTableView();

        } catch (Exception e) {
            new ErrorDialog(shell, "Error", "Error getting fields", e);
        }
    }
}
