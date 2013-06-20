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

/**
 * @author rob@simplymeasured.com
 * @since 6/20/13
 */
public class HiveUtilsTest {
    @Test
    public void testEscapingLineFeed() {
        String unescaped = "Test\ntest\ntest";
        String escaped = "Test\\\\ntest\\\\ntest";

        Assert.assertEquals(escaped, HiveUtils.escapeString(unescaped));
    }

    @Test
    public void testEscapingCarriageReturn() {
        String unescaped = "Test\rtest\rtest";
        String escaped = "Test\\\\rtest\\\\rtest";

        Assert.assertEquals(escaped, HiveUtils.escapeString(unescaped));
    }

    @Test
    public void testEscapingTab() {
        String unescaped = "Test\ttest\ttest";
        String escaped = "Test\\\\ttest\\\\ttest";

        Assert.assertEquals(escaped, HiveUtils.escapeString(unescaped));
    }

    @Test
    public void testEscapingMixed() {
        String unescaped = "Test\ttest\ntest\rtest";
        String escaped = "Test\\\\ttest\\\\ntest\\\\rtest";

        Assert.assertEquals(escaped, HiveUtils.escapeString(unescaped));
    }
}
