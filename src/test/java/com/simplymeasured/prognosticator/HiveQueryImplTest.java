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

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

/**
 * @author rob@simplymeasured.com
 * @since 6/24/13
 */
@RunWith(PowerMockRunner.class)
public class HiveQueryImplTest {
    @Test
    public void testRunQuery() throws Exception {
        final String queryString = "SELECT * FROM foo";

        NamedParameterJdbcTemplate template = mock(NamedParameterJdbcTemplate.class);

        HiveQueryImpl query = new HiveQueryImpl(template);

        Map<String, Object> parameterMap = new HashMap<String, Object>() {
            {
                put("param1", "string");
                put("param2", 123);
            }
        };

        SqlRowSet rowSet = mock(SqlRowSet.class);
        when(template.queryForRowSet(queryString, parameterMap)).thenReturn(rowSet);

        QueryCursor<Map<String, Object>> cursor = query.runQuery(queryString, parameterMap);

        Assert.assertNotNull(cursor);
    }
}
