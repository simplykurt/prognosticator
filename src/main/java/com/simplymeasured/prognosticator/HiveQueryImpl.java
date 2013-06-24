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

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import javax.sql.DataSource;
import java.util.Map;

/**
 * Provides an implementation to execute a query against Hive.
 *
 * @author rob@simplymeasured.com
 * @since 6/24/13
 */
public class HiveQueryImpl implements HiveQuery {
    private NamedParameterJdbcTemplate jdbcTemplate;

    public HiveQueryImpl(DataSource dataSource) {
        this.jdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
    }

    public HiveQueryImpl(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public QueryCursor<Map<String, Object>> runQuery(String queryStatement, Map<String, Object> parameters) {
        SqlRowSet rowSet = jdbcTemplate.queryForRowSet(queryStatement, parameters);

        return new HiveQueryCursorImpl(rowSet);
    }
}
