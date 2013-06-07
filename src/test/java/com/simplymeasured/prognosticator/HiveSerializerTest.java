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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hcatalog.api.HCatTable;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayOutputStream;
import java.util.Map;

import static org.mockito.Mockito.*;

/**
 * @author rob@simplymeasured.com
 * @since 6/3/13
 */
@RunWith(PowerMockRunner.class)
public class HiveSerializerTest {
    @Test
    public void testSerializeMap() throws Exception {
        HCatTable table = mock(HCatTable.class);

        HCatSchema mapSchema = new HCatSchema(Lists.newArrayList(
                getSubFieldSchema("valueField", HCatFieldSchema.Type.BIGINT)));
        HCatFieldSchema fieldSchema = new HCatFieldSchema("testField",
                HCatFieldSchema.Type.MAP,
                HCatFieldSchema.Type.STRING,
                mapSchema, "");

        HiveSerializer serializer = new HiveSerializer(table);

        Map<String, Long> testMap = Maps.newLinkedHashMap();
        testMap.put("field1", 123L);
        testMap.put("field2", 456L);

        byte[] result = serializer.serializeMap(fieldSchema, testMap, 1);

        ByteArrayOutputStream expectedResult = new ByteArrayOutputStream();
        expectedResult.write(Bytes.toBytes("field1"));
        expectedResult.write('\003');
        expectedResult.write(Bytes.toBytes(123L));
        expectedResult.write('\002');
        expectedResult.write(Bytes.toBytes("field2"));
        expectedResult.write('\003');
        expectedResult.write(Bytes.toBytes(456L));

        byte[] expectedResultBytes = expectedResult.toByteArray();

        Assert.assertArrayEquals(expectedResultBytes, result);
    }

    /**
     * Ensure we can do a basic serialization of a STRUCT
     *
     * @throws Exception
     */
    @Test
    public void testSerializeStruct() throws Exception {
        HCatTable table = mock(HCatTable.class);

        HCatSchema structSchema = new HCatSchema(Lists.newArrayList(
                getSubFieldSchema("intField", HCatFieldSchema.Type.BIGINT),
                getSubFieldSchema("strField", HCatFieldSchema.Type.STRING)));
        HCatFieldSchema fieldSchema = new HCatFieldSchema("testField", HCatFieldSchema.Type.STRUCT, structSchema, "");

        HiveSerializer serializer = new HiveSerializer(table);

        Map<String, Object> testMap = Maps.newHashMap();
        testMap.put("intField", 123L);
        testMap.put("strField", "This is a string");

        byte[] result = serializer.serializeStruct(fieldSchema, testMap, 1);

        ByteArrayOutputStream expectedResult = new ByteArrayOutputStream();
        expectedResult.write(Bytes.toBytes(123L));
        expectedResult.write('\002');
        expectedResult.write(Bytes.toBytes("This is a string"));

        Assert.assertArrayEquals(expectedResult.toByteArray(), result);
    }

    /**
     * Ensure we throw an exception when a field within a STRUCT is null, as this is not allowed.
     *
     * @throws Exception
     */
    @Test(expected=IllegalArgumentException.class)
    public void testSerializeStructWithNullField() throws Exception {
        HCatTable table = mock(HCatTable.class);

        HCatSchema structSchema = new HCatSchema(Lists.newArrayList(
                getSubFieldSchema("intField", HCatFieldSchema.Type.BIGINT),
                getSubFieldSchema("strField", HCatFieldSchema.Type.STRING)));
        HCatFieldSchema fieldSchema = new HCatFieldSchema("testField", HCatFieldSchema.Type.STRUCT, structSchema, "");

        HiveSerializer serializer = new HiveSerializer(table);

        Map<String, Object> testMap = Maps.newHashMap();

        serializer.serializeStruct(fieldSchema, testMap, 1);
    }

    protected HCatFieldSchema getSubFieldSchema(String fieldName, HCatFieldSchema.Type type) throws HCatException {
        HCatFieldSchema result = new HCatFieldSchema(fieldName, type, "");

        return result;
    }
}
