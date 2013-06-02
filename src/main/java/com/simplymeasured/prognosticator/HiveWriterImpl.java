/*
 * Copyright 2013 Simply Measured, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.simplymeasured.prognosticator;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hcatalog.api.HCatClient;
import org.apache.hcatalog.api.HCatTable;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.springframework.beans.factory.annotation.Required;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * @author rob@simplymeasured.com
 * @since 5/2/13
 */
public class HiveWriterImpl implements HiveWriter {
    private static final Log LOG = LogFactory.getLog(HiveWriterImpl.class);

    private HCatClient hcatClient;
    private Configuration hbaseConfiguration;

    private Cache<String, HCatTable> tableHandleCache;

    public HiveWriterImpl() {
        this.tableHandleCache = CacheBuilder.newBuilder()
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build();
    }

    @Override
    public void writeRow(final String tableName, Map<String, Object> entity) throws Exception {
        HCatTable table = tableHandleCache.get(tableName, new Callable<HCatTable>() {
            @Override
            public HCatTable call() throws Exception {
                return hcatClient.getTable("default", tableName);
            }
        });

        String hbaseTableName = HiveUtils.getTableName(table);

        HTableInterface tableInterface = new HTable(hbaseConfiguration, tableName);
        tableInterface.setAutoFlush(true);

        Serializer serializer = new Serializer(table);

        try {
            List<HCatFieldSchema> columns = table.getCols();

            HCatFieldSchema keyColumn = columns.get(0);

            Put put;

            final byte[] rowkey;

            final List<String> keyRequiredColumns = Lists.newArrayList();

            if(keyColumn.getType() == HCatFieldSchema.Type.STRUCT) {
                HCatSchema keySchema = keyColumn.getStructSubSchema();

                for(HCatFieldSchema fieldSchema: keySchema.getFields()) {
                    keyRequiredColumns.add(fieldSchema.getName());
                }

                rowkey = serializer.serializeHiveType(keyColumn, null, entity, 1);
            } else {
                keyRequiredColumns.add(keyColumn.getName());

                rowkey = serializer.serializeHiveType(keyColumn, null, entity.get(keyColumn.getName()), 1);
            }

            if(rowkey == null || rowkey.length == 0) {
                throw new IllegalArgumentException(String.format("Rowkey is null, required key fields missing: %s",
                        keyRequiredColumns));
            }

            put = new Put(rowkey);

            for(int i = 1; i < columns.size(); i++) {
                HCatFieldSchema columnSchema = columns.get(i);

                serializer.serialize(columnSchema, i, put, entity.get(columnSchema.getName()), 1);
            }

            tableInterface.put(put);
        } finally {
            tableInterface.close();
        }
    }

    @Required
    public void setHCatClient(HCatClient hcatClient) {
        this.hcatClient = hcatClient;
    }

    @Required
    public void setHBaseConfiguration(Configuration hbaseConfiguration) {
        this.hbaseConfiguration = hbaseConfiguration;
    }

    protected static class Serializer {
        protected byte[] separators;
        private HCatTable table;

        public Serializer(HCatTable table) {
            this.table = table;

            this.separators = HiveUtils.getSeparators(table);
        }

        /**
         * Serialize an object into an HBase put
         *
         * @param field field from the schema to serialize
         * @param mappingPosition where it's at in the column mapping
         * @param put the HBase Put object
         * @param object what to serialize
         * @param level the recursion level - sets up field separators properly. 1-based.
         * @throws java.io.IOException
         */
        protected void serialize(HCatFieldSchema field, int mappingPosition, Put put, Object object, int level)
                throws IOException {
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

            put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), result);
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
                    {
                        char separator = (char) separators[level];

                        HCatFieldSchema arrayFieldSchema = field.getArrayElementSchema().getFields().get(0);
                        List list = (List)object;

                        ByteArrayOutputStream baos = new ByteArrayOutputStream();

                        boolean first = true;

                        for(Object obj: list) {
                            if(!first) {
                                baos.write(separator);
                            }

                            baos.write(serializeHiveType(arrayFieldSchema, arrayFieldSchema.getType(), obj, level + 1));

                            first = false;
                        }

                        result = baos.toByteArray();

                        break;
                    }
                    case MAP:
                    {
                        char separator = (char) separators[level];
                        char keyValueSeparator = (char) separators[level+1];

                        Map mapData = (Map)object;

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

                        result = baos.toByteArray();

                        break;
                    }
                    case STRUCT:
                    {
                        char separator = (char)separators[level];

                        Map structData = (Map)object;

                        ByteArrayOutputStream baos = new ByteArrayOutputStream();

                        HCatSchema structSchema = field.getStructSubSchema();

                        boolean first = true;

                        for(HCatFieldSchema structField: structSchema.getFields()) {
                            if(!first) {
                                baos.write(separator);
                            }

                            Object obj = structData.get(structField.getName());

                            baos.write(serializeHiveType(structField, null, obj, level + 1));

                            first = false;
                        }

                        result = baos.toByteArray();

                        break;
                    }
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
                        result = Bytes.toBytes((String)object);
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
    }
}
