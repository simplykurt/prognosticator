/*
 * Copyright 2013 Simply Measured, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.simplymeasured.prognosticator;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hcatalog.api.HCatTable;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Serialize a given Map into a Hive-compatible structure within a HBase Put
 *
 * @author rob@simplymeasured.com
 * @since 6/3/13
 */
public class HiveSerializer {
    private static final Log LOG = LogFactory.getLog(HiveSerializer.class);

    protected byte[] separators;
    private HCatTable table;
    private final char escapeChar = '\\';
    private final boolean[] needsEscape = new boolean[128];

    public HiveSerializer(HCatTable table) {
        this.table = table;

        this.separators = HiveUtils.getSeparators(table);

        for (int i = 0; i < 128; i++) {
            this.needsEscape[i] = false;
        }
        this.needsEscape[this.escapeChar] = true;
        this.needsEscape[10] = true; // this is \n
        for (int i = 0; i < this.separators.length; i++) {
            this.needsEscape[this.separators[i]] = true;
        }

        for(int i = 0; i < 32; i++) {
            this.needsEscape[i] = true;
        }
    }

    /**
     * Serialize an object into an HBase put and/or delete
     *
     * Any fields that have a NULL value will get added to the Delete object
     *
     * @param field field from the schema to serialize
     * @param mappingPosition where it's at in the column mapping
     * @param put the HBase Put object
     * @param delete the HBase Delete object
     * @param object what to serialize
     * @param level the recursion level - sets up field separators properly. 1-based.
     * @throws java.io.IOException
     */
    protected void serialize(HCatFieldSchema field, int mappingPosition, Put put, Delete delete,
                             Object object, int level) throws IOException {
        assert level > 0;

        List<String> columnMappings = HiveUtils.getColumnMappings(table);

        // column family is determined by mapping
        final String columnFamily;
        // column name is mostly determined by the mapping, unless
        // we're dealing with a Hive MAP, at that case, the column name
        // is the key in the map
        String columnName = "";

        if(columnMappings != null) {
            String[] mappingInfo = columnMappings.get(mappingPosition).split(":");
            columnFamily = mappingInfo[0];

            if(mappingInfo.length > 1) {
                columnName = mappingInfo[1];
            }
        } else {
            columnFamily = "default";
            columnName = field.getName();
        }

        byte[] result = serializeHiveType(field, null, object, level);

        byte[] columnFamilyBytes = Bytes.toBytes(columnFamily);
        byte[] columnQualifierBytes = Bytes.toBytes(columnName);

        if(result == null) {
            delete.deleteColumn(columnFamilyBytes, columnQualifierBytes);
        } else {
            put.add(columnFamilyBytes, columnQualifierBytes, result);
        }
    }

    protected byte[] serializeHiveType(HCatFieldSchema field, HCatFieldSchema.Type customType, Object object,
                                       int level) throws IOException {
        HCatFieldSchema.Type type = customType != null ? customType : field.getType();

        byte[] result;

        if(object == null)
            return null;

        try {
            switch (type) {
                case ARRAY:
                    result = serializeArray(field, (List) object, level);
                    break;
                case MAP:
                    result = serializeMap(field, (Map) object, level);
                    break;
                case STRUCT:
                    result = serializeStruct(field, (Map) object, level);
                    break;
                case BIGINT:
                    result = Bytes.toBytes(PutHelper.valueAsLong(object));
                    break;
                case BINARY:
                    result = (byte[])object;
                    break;
                case BOOLEAN:
                    result = Bytes.toBytes((Boolean)object);
                    break;
                case DOUBLE:
                    result = Bytes.toBytes(PutHelper.valueAsDouble(object));
                    break;
                case FLOAT:
                    result = Bytes.toBytes(PutHelper.valueAsFloat(object));
                    break;
                case INT:
                    result = Bytes.toBytes(PutHelper.valueAsInteger(object));
                    break;
                case SMALLINT:
                    result = Bytes.toBytes((Short)object);
                    break;
                case STRING:
                    result = Bytes.toBytes(HiveUtils.escapeString((String)object));
                    break;
                case TINYINT:
                    result = Bytes.toBytes((Byte)object);
                    break;
                default:
                    throw new IllegalArgumentException("unsupported type");
            }
        } catch(ClassCastException cce) {
            LOG.warn(String.format("Column %s expected type %s but found %s - value = %s",
                    field.getName(), type, object.getClass().getName(), object));

            throw cce;
        }

        return result;
    }

    protected byte[] serializeArray(HCatFieldSchema field, List list, int level) throws IOException {
        char separator = (char) separators[level];

        HCatFieldSchema arrayFieldSchema = field.getArrayElementSchema().getFields().get(0);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        boolean first = true;

        for(Object obj: list) {
            if(!first) {
                baos.write(separator);
            }

            baos.write(serializeHiveType(arrayFieldSchema, arrayFieldSchema.getType(), obj, level + 1));

            first = false;
        }

        return baos.toByteArray();
    }

    protected byte[] serializeMap(HCatFieldSchema field, Map mapData, int level) throws IOException {
        char separator = (char) separators[level];
        char keyValueSeparator = (char) separators[level+1];

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        boolean first = true;

        for(Object mapKey : mapData.keySet()) {
            if(!first) {
                baos.write(separator);
            }

            HCatFieldSchema.Type mapKeyType = field.getMapKeyType();
            HCatFieldSchema mapFieldSchema = field.getMapValueSchema().getFields().get(0);

            baos.write(serializeHiveType(mapFieldSchema, mapKeyType,  mapKey, level + 2));
            baos.write(keyValueSeparator);
            baos.write(serializeHiveType(mapFieldSchema, null, mapData.get(mapKey), level + 2));

            first = false;
        }

        return baos.toByteArray();
    }

    protected byte[] serializeStruct(HCatFieldSchema field, Map structData, int level) throws IOException {
        char separator = (char)separators[level];

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        HCatSchema structSchema = field.getStructSubSchema();

        boolean first = true;

        for(HCatFieldSchema structField: structSchema.getFields()) {
            if(!first) {
                baos.write(separator);
            }

            Object obj = structData.get(structField.getName());

            if(obj == null) {
                throw new IllegalArgumentException(
                        String.format("STRUCT types cannot have null members - field %s",
                                structField.getName()));
            }

            baos.write(serializeHiveType(structField, null, obj, level + 1));

            first = false;
        }

        return baos.toByteArray();
    }
}
