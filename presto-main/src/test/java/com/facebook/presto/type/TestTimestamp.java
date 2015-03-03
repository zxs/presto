/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.type;

import com.facebook.presto.Session;
import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlTime;
import com.facebook.presto.spi.type.SqlTimeWithTimeZone;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.SqlTimestampWithTimeZone;
import com.facebook.presto.spi.type.TimeZoneKey;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKey;
import static com.facebook.presto.util.DateTimeZoneIndex.getDateTimeZone;
import static java.util.Locale.ENGLISH;
import static org.joda.time.DateTimeZone.UTC;

public class TestTimestamp
{
    private static final TimeZoneKey TIME_ZONE_KEY = getTimeZoneKey("Europe/Berlin");
    private static final DateTimeZone DATE_TIME_ZONE = getDateTimeZone(TIME_ZONE_KEY);

    private FunctionAssertions functionAssertions;

    @BeforeClass
    public void setUp()
    {
        Session session = Session.builder()
                .setUser("user")
                .setSource("test")
                .setCatalog("catalog")
                .setSchema("schema")
                .setTimeZoneKey(TIME_ZONE_KEY)
                .setLocale(ENGLISH)
                .build();
        functionAssertions = new FunctionAssertions(session);
    }

    private void assertFunction(String projection, Object expected)
    {
        functionAssertions.assertFunction(projection, expected);
    }

    @Test
    public void testLiteral()
    {
        assertFunction("TIMESTAMP '2013-03-30 01:05'", new SqlTimestamp(new DateTime(2013, 3, 30, 1, 5, 0, 0, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2013-03-30 02:05'", new SqlTimestamp(new DateTime(2013, 3, 30, 2, 5, 0, 0, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2013-03-30 03:05'", new SqlTimestamp(new DateTime(2013, 3, 30, 3, 5, 0, 0, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY));

        assertFunction("TIMESTAMP '2001-01-22 03:04:05.321'", new SqlTimestamp(new DateTime(2001, 1, 22, 3, 4, 5, 321, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-01-22 03:04:05'", new SqlTimestamp(new DateTime(2001, 1, 22, 3, 4, 5, 0, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-01-22 03:04'", new SqlTimestamp(new DateTime(2001, 1, 22, 3, 4, 0, 0, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-01-22'", new SqlTimestamp(new DateTime(2001, 1, 22, 0, 0, 0, 0, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY));

        assertFunction("TIMESTAMP '2001-1-2 3:4:5.321'", new SqlTimestamp(new DateTime(2001, 1, 2, 3, 4, 5, 321, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-2 3:4:5'", new SqlTimestamp(new DateTime(2001, 1, 2, 3, 4, 5, 0, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-2 3:4'", new SqlTimestamp(new DateTime(2001, 1, 2, 3, 4, 0, 0, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("TIMESTAMP '2001-1-2'", new SqlTimestamp(new DateTime(2001, 1, 2, 0, 0, 0, 0, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY));
    }

    @Test
    public void testEqual()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' = TIMESTAMP '2001-1-22 03:04:05.321'", true);
        assertFunction("TIMESTAMP '2001-1-22' = TIMESTAMP '2001-1-22'", true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' = TIMESTAMP '2001-1-22 03:04:05.333'", false);
        assertFunction("TIMESTAMP '2001-1-22' = TIMESTAMP '2001-1-11'", false);
    }

    @Test
    public void testNotEqual()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' <> TIMESTAMP '2001-1-22 03:04:05.333'", true);
        assertFunction("TIMESTAMP '2001-1-22' <> TIMESTAMP '2001-1-11'", true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' <> TIMESTAMP '2001-1-22 03:04:05.321'", false);
        assertFunction("TIMESTAMP '2001-1-22' <> TIMESTAMP '2001-1-22'", false);
    }

    @Test
    public void testLessThan()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' < TIMESTAMP '2001-1-22 03:04:05.333'", true);
        assertFunction("TIMESTAMP '2001-1-22' < TIMESTAMP '2001-1-23'", true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' < TIMESTAMP '2001-1-22 03:04:05.321'", false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' < TIMESTAMP '2001-1-22 03:04:05'", false);
        assertFunction("TIMESTAMP '2001-1-22' < TIMESTAMP '2001-1-22'", false);
        assertFunction("TIMESTAMP '2001-1-22' < TIMESTAMP '2001-1-20'", false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' <= TIMESTAMP '2001-1-22 03:04:05.333'", true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' <= TIMESTAMP '2001-1-22 03:04:05.321'", true);
        assertFunction("TIMESTAMP '2001-1-22' <= TIMESTAMP '2001-1-23'", true);
        assertFunction("TIMESTAMP '2001-1-22' <= TIMESTAMP '2001-1-22'", true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' <= TIMESTAMP '2001-1-22 03:04:05'", false);
        assertFunction("TIMESTAMP '2001-1-22' <= TIMESTAMP '2001-1-20'", false);
    }

    @Test
    public void testGreaterThan()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' > TIMESTAMP '2001-1-22 03:04:05.111'", true);
        assertFunction("TIMESTAMP '2001-1-22' > TIMESTAMP '2001-1-11'", true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' > TIMESTAMP '2001-1-22 03:04:05.321'", false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' > TIMESTAMP '2001-1-22 03:04:05.333'", false);
        assertFunction("TIMESTAMP '2001-1-22' > TIMESTAMP '2001-1-22'", false);
        assertFunction("TIMESTAMP '2001-1-22' > TIMESTAMP '2001-1-23'", false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' >= TIMESTAMP '2001-1-22 03:04:05.111'", true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' >= TIMESTAMP '2001-1-22 03:04:05.321'", true);
        assertFunction("TIMESTAMP '2001-1-22' >= TIMESTAMP '2001-1-11'", true);
        assertFunction("TIMESTAMP '2001-1-22' >= TIMESTAMP '2001-1-22'", true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' >= TIMESTAMP '2001-1-22 03:04:05.333'", false);
        assertFunction("TIMESTAMP '2001-1-22' >= TIMESTAMP '2001-1-23'", false);
    }

    @Test
    public void testBetween()
    {
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' between TIMESTAMP '2001-1-22 03:04:05.111' and TIMESTAMP '2001-1-22 03:04:05.333'", true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' between TIMESTAMP '2001-1-22 03:04:05.321' and TIMESTAMP '2001-1-22 03:04:05.333'", true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' between TIMESTAMP '2001-1-22 03:04:05.111' and TIMESTAMP '2001-1-22 03:04:05.321'", true);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' between TIMESTAMP '2001-1-22 03:04:05.321' and TIMESTAMP '2001-1-22 03:04:05.321'", true);

        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' between TIMESTAMP '2001-1-22 03:04:05.322' and TIMESTAMP '2001-1-22 03:04:05.333'", false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' between TIMESTAMP '2001-1-22 03:04:05.311' and TIMESTAMP '2001-1-22 03:04:05.312'", false);
        assertFunction("TIMESTAMP '2001-1-22 03:04:05.321' between TIMESTAMP '2001-1-22 03:04:05.333' and TIMESTAMP '2001-1-22 03:04:05.111'", false);
    }

    @Test
    public void testCastToDate()
            throws Exception
    {
        long millis = new DateTime(2001, 1, 22, 0, 0, UTC).getMillis();
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321' as date)", new SqlDate((int) TimeUnit.MILLISECONDS.toDays(millis)));
    }

    @Test
    public void testCastToTime()
            throws Exception
    {
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321' as time)", new SqlTime(new DateTime(1970, 1, 1, 3, 4, 5, 321, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY));
    }

    @Test
    public void testCastToTimeWithTimeZone()
            throws Exception
    {
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321' as time with time zone)",
                new SqlTimeWithTimeZone(new DateTime(1970, 1, 1, 3, 4, 5, 321, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY));
    }

    @Test
    public void testCastToTimestampWithTimeZone()
    {
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321' as timestamp with time zone)",
                new SqlTimestampWithTimeZone(new DateTime(2001, 1, 22, 3, 4, 5, 321, DATE_TIME_ZONE).getMillis(), DATE_TIME_ZONE.toTimeZone()));
    }

    @Test
    public void testCastToSlice()
    {
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321' as varchar)", "2001-01-22 03:04:05.321");
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05' as varchar)", "2001-01-22 03:04:05.000");
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04' as varchar)", "2001-01-22 03:04:00.000");
        assertFunction("cast(TIMESTAMP '2001-1-22' as varchar)", "2001-01-22 00:00:00.000");
    }

    @Test
    public void testCastFromSlice()
    {
        assertFunction("cast('2001-1-22 03:04:05.321' as timestamp) = TIMESTAMP '2001-01-22 03:04:05.321'", true);
        assertFunction("cast('2001-1-22 03:04:05' as timestamp) = TIMESTAMP '2001-01-22 03:04:05.000'", true);
        assertFunction("cast('2001-1-22 03:04' as timestamp) = TIMESTAMP '2001-01-22 03:04:00.000'", true);
        assertFunction("cast('2001-1-22' as timestamp) = TIMESTAMP '2001-01-22 00:00:00.000'", true);
    }

    @Test
    public void testGreatest()
            throws Exception
    {
        assertFunction("greatest(TIMESTAMP '2013-03-30 01:05', TIMESTAMP '2012-03-30 01:05')", new SqlTimestamp(new DateTime(2013, 3, 30, 1, 5, 0, 0, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("greatest(TIMESTAMP '2013-03-30 01:05', TIMESTAMP '2012-03-30 01:05', TIMESTAMP '2012-05-01 01:05')", new SqlTimestamp(new DateTime(2013, 3, 30, 1, 5, 0, 0, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY));
    }

    @Test
    public void testLeast()
            throws Exception
    {
        assertFunction("least(TIMESTAMP '2013-03-30 01:05', TIMESTAMP '2012-03-30 01:05')", new SqlTimestamp(new DateTime(2012, 3, 30, 1, 5, 0, 0, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY));
        assertFunction("least(TIMESTAMP '2013-03-30 01:05', TIMESTAMP '2012-03-30 01:05', TIMESTAMP '2012-05-01 01:05')", new SqlTimestamp(new DateTime(2012, 3, 30, 1, 5, 0, 0, DATE_TIME_ZONE).getMillis(), TIME_ZONE_KEY));
    }
}
