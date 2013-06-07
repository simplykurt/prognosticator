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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hcatalog.api.HCatTable;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * @author rob@simplymeasured.com
 * @since 6/3/13
 */
public class HiveDeserializer {
    protected byte[] separators;
    private HCatTable table;
    private Result dbResult;

    public HiveDeserializer(HCatTable table, Result dbResult) {
        this.table = table;
        this.dbResult = dbResult;

        this.separators = HiveUtils.getSeparators(table);
    }

    public Map<String, Object> deserialize() throws IOException {
        Map<String, Object> result = Maps.newHashMap();

        List<String> columnMappings = HiveUtils.getColumnMappings(table);

        List<HCatFieldSchema> columns = table.getCols();

        HCatFieldSchema identifier = columns.get(0);
        result.put(identifier.getName(), deserializeHiveType(identifier, null, dbResult.getRow(), 1));

        for(int i = 1; i < columns.size(); i++) {
            HCatFieldSchema field = columns.get(i);

            // column family is determined by mapping
            final String columnFamily;
            // column name is mostly determined by the mapping, unless
            // we're dealing with a Hive MAP, at that case, the column name
            // is the key in the map
            String columnName = "";

            if(columnMappings != null) {
                String[] mappingInfo = columnMappings.get(i).split(":");
                columnFamily = mappingInfo[0];

                if(mappingInfo.length > 1) {
                    columnName = mappingInfo[1];
                }
            } else {
                columnFamily = "default";
                columnName = field.getName();
            }

            NavigableMap<byte[], byte[]> familyMap = dbResult.getFamilyMap(Bytes.toBytes(columnFamily));

            if(familyMap != null && !familyMap.isEmpty()) {
                result.put(columnName, deserializeHiveType(field, null, familyMap.get(Bytes.toBytes(columnName)),
                        1));
            } else {
                result.put(columnName, null);
            }
        }

        return result;
    }

    /**
     * Deserialize an HBase byte array to an object per the HCatalog schema
     *
     * @param field field to deserialize
     * @param customType custom overridden type (used during recursion) - can be null
     * @param object the byte array to deserialize
     * @param level the recursion level - sets up field separators properly. 1-based.
     * @return deserialized object
     * @throws java.io.IOException
     */
    @SuppressWarnings("unchecked")
    public Object deserializeHiveType(HCatFieldSchema field, HCatFieldSchema.Type customType, byte[] object,
                                      int level) throws IOException {
        assert level > 0;

        // handle the null case properly.
        if(object == null)
            return null;

        Object result;

        HCatFieldSchema.Type type = customType != null ? customType : field.getType();

        switch(type) {
            case ARRAY:
                result = deserializeArray(field, object, level);
                break;
            case MAP:
                result = deserializeMap(field, object, level);
                break;
            case STRUCT:
                result = deserializeStruct(field, object, level);
                break;
            case BIGINT:
                result = Bytes.toLong(object);
                break;
            case BINARY:
                result = object;
                break;
            case BOOLEAN:
                result = Bytes.toBoolean(object);
                break;
            case DOUBLE:
                result = Bytes.toDouble(object);
                break;
            case FLOAT:
                result = Bytes.toFloat(object);
                break;
            case INT:
                result = Bytes.toInt(object);
                break;
            case SMALLINT:
                result = Bytes.toShort(object);
                break;
            case STRING:
                result = HiveUtils.unescapeString(Bytes.toString(object));
                break;
            case TINYINT:
                result = object[0];
                break;
            default:
                throw new IllegalArgumentException("unsupported type");
        }

        return result;
    }

    private Object deserializeStruct(HCatFieldSchema field, byte[] object, int level) throws IOException {
        char separator = (char)separators[level];

        Map structData = Maps.newHashMap();

        HCatSchema structSchema = field.getStructSubSchema();

        ByteArrayInputStream inputBuffer = new ByteArrayInputStream(object);
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        for(HCatFieldSchema structField: structSchema.getFields()) {
            int read = inputBuffer.read();
            while(read > -1) {
                byte b = (byte)read;

                if(b == separator) {
                    Object value = deserializeHiveType(structField, null, buffer.toByteArray(), level + 1);

                    structData.put(structField.getName(), value);

                    buffer.reset();

                    // break to the outer loop here, we've finished this field.
                    break;
                } else {
                    buffer.write(b);
                }

                read = inputBuffer.read();
            }

            if(buffer.size() > 0) {
                Object value = deserializeHiveType(structField, null, buffer.toByteArray(), level + 1);

                structData.put(structField.getName(), value);

                buffer.reset();
            }
        }

        return structData;
    }

    private Object deserializeMap(HCatFieldSchema field, byte[] object, int level) throws IOException {
        char separator = (char) separators[level];
        char keyValueSeparator = (char) separators[level+1];

        Map mapData = Maps.newHashMap();

        ByteArrayInputStream inputBuffer = new ByteArrayInputStream(object);
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        Object key = null;

        HCatFieldSchema.Type mapKeyType = field.getMapKeyType();
        HCatFieldSchema mapFieldSchema = field.getMapValueSchema().getFields().get(0);

        int read = inputBuffer.read();
        while(read > -1) {
            byte b = (byte)read;

            if(b == separator) {
                // end of the value

                Object value = deserializeHiveType(mapFieldSchema, null, buffer.toByteArray(), level + 2);
                mapData.put(key, value);
                buffer.reset();
            } else if(b == keyValueSeparator) {
                key = deserializeHiveType(mapFieldSchema, mapKeyType, buffer.toByteArray(), level + 2);

                buffer.reset();
            } else {
                buffer.write(b);
            }

            read = inputBuffer.read();
        }

        if(buffer.size() > 0) {
            Object value = deserializeHiveType(mapFieldSchema, null, buffer.toByteArray(), level + 2);
            mapData.put(key, value);
            buffer.reset();
        }

        return mapData;
    }

    private Object deserializeArray(HCatFieldSchema field, byte[] object, int level) throws IOException {
        char separator = (char) separators[level];

        HCatFieldSchema arrayFieldSchema = field.getArrayElementSchema().getFields().get(0);

        List list = Lists.newArrayList();

        ByteArrayInputStream inputBuffer = new ByteArrayInputStream(object);

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        int read = inputBuffer.read();
        while(read > -1) {
            byte b = (byte)read;

            if(b == separator) {
                list.add(deserializeHiveType(arrayFieldSchema, null, buffer.toByteArray(), level + 1));

                buffer.reset();
            } else {
                buffer.write(b);
            }

            read = inputBuffer.read();
        }

        if(buffer.size() > 0) {
            list.add(deserializeHiveType(arrayFieldSchema, null, buffer.toByteArray(), level + 1));
            buffer.reset();
        }

        return list;
    }
}
