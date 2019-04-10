/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.loader;


import org.junit.Test;

import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.TimeZone;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class LoaderTimestampIT extends LoaderBase
{
  @Test
  public void testLoadTimestamp() throws Exception
  {
    final String targetTableName = "LOADER_TEST_TIMESTAMP";

    // create table including TIMESTAMP_NTZ
    testConnection.createStatement().execute(String.format(
        "CREATE OR REPLACE TABLE %s ("
        + "ID int, "
        + "C1 varchar(255), "
        + "C2 timestamp_ntz)", targetTableName));

    // Binding java.util.Date, Timestamp and java.sql.Date with TIMESTAMP
    // datatype. No java.sql.Time binding is supported for TIMESTAMP.
    // For java.sql.Time, the target data type must be TIME.
    Object[] testData = new Object[]{
        new Date(),
        java.sql.Timestamp.valueOf("0001-01-01 08:00:00"),
        java.sql.Date.valueOf("2001-01-02")
    };

    for (int i = 0; i < 2; ++i)
    {
      boolean useLocalTimezone = false;
      TimeZone originalTimeZone;
      TimeZone targetTimeZone;

      if (i == 0)
      {
        useLocalTimezone = true;
        originalTimeZone = TimeZone.getDefault();
        targetTimeZone = TimeZone.getTimeZone("America/Los_Angeles");
      }
      else
      {
        originalTimeZone = TimeZone.getTimeZone("UTC");
        targetTimeZone = TimeZone.getTimeZone("UTC");
      }

      // input timestamp associated with the target timezone, America/Los_Angeles
      for (Object testTs : testData)
      {
        _testLoadTimestamp(targetTableName, originalTimeZone,
                           targetTimeZone, testTs, useLocalTimezone, false);
      }
    }
  }

  private void _testLoadTimestamp(
      String targetTableName,
      TimeZone originalTimeZone, TimeZone targetTimeZone,
      Object testTs, boolean useLocalTimeZone,
      boolean mapTimeToTimestamp) throws Exception
  {
    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(
        testConnection, putConnection);

    tdcb
        .setStartTransaction(true)
        .setTruncateTable(true)
        .setTableName(targetTableName)
        .setUseLocalTimezone(useLocalTimeZone)
        .setMapTimeToTimestamp(mapTimeToTimestamp)
        .setColumns(Arrays.asList(
            "ID", "C1", "C2"
        ));
    StreamLoader loader = tdcb.getStreamLoader();
    TestDataConfigBuilder.ResultListener listener = tdcb.getListener();

    TimeZone.setDefault(targetTimeZone); // change default timezone before start

    loader.start();

    for (int i = 0; i < 5; ++i)
    {
      Object[] row = new Object[]{
          i, "foo_" + i, testTs
      };
      loader.submitRow(row);
    }
    loader.finish();
    TimeZone.setDefault(originalTimeZone);

    assertThat("Loader detected errors",
               listener.getErrorCount(), equalTo(0));

    ResultSet rs = testConnection.createStatement().executeQuery(
        String.format("SELECT * FROM \"%s\"", targetTableName));

    rs.next();
    Timestamp ts = rs.getTimestamp("C2");

    // format the input TS with the target timezone
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    sdf.setTimeZone(targetTimeZone);
    String currenTsStr = sdf.format(testTs);

    // format the retrieved TS with the original timezone
    sdf.setTimeZone(originalTimeZone);
    String retrievedTsStr = sdf.format(new Date(ts.getTime()));

    // They must be identical.
    assertThat("Input and retrieved timestamp are different",
               retrievedTsStr, equalTo(currenTsStr));
  }

  @Test
  public void testLoadTimestampV1() throws Exception
  {
    final String targetTableName = "LOADER_TEST_TIMESTAMP_V1";

    // create table including TIMESTAMP_NTZ
    testConnection.createStatement().execute(String.format(
        "CREATE OR REPLACE TABLE %s ("
        + "ID int, "
        + "C1 varchar(255), "
        + "C2 timestamp_ntz)", targetTableName));

    // Binding java.sql.Time with TIMESTAMP is supported only if
    // mapTimeToTimestamp flag is enabled. This is required to keep the
    // old behavior of Informatica V1 connector.
    Object[] testData = new Object[]{
        // full timestamp in Time object. Interestingly all values are
        // preserved.
        new java.sql.Time(1502931205000L),
        java.sql.Time.valueOf("12:34:56") // a basic test case
    };

    for (int i = 0; i < 2; ++i)
    {
      boolean useLocalTimezone;
      TimeZone originalTimeZone;
      TimeZone targetTimeZone;

      if (i == 0)
      {
        useLocalTimezone = true;
        originalTimeZone = TimeZone.getDefault();
        targetTimeZone = TimeZone.getTimeZone("America/Los_Angeles");
      }
      else
      {
        useLocalTimezone = false;
        originalTimeZone = TimeZone.getTimeZone("UTC");
        targetTimeZone = TimeZone.getTimeZone("UTC");
      }

      // input timestamp associated with the target timezone, America/Los_Angeles
      for (Object testTs : testData)
      {
        _testLoadTimestamp(targetTableName, originalTimeZone,
                           targetTimeZone, testTs, useLocalTimezone, true);
      }
    }
  }

  @Test
  public void testLoadTimestampMilliseconds() throws Exception
  {
    String srcTable = "LOAD_TIMESTAMP_MS_SRC";
    String dstTable = "LOAD_TIMESTAMP_MS_DST";

    testConnection.createStatement().execute(
        String.format("create or replace table %s(c1 int, c2 timestamp_ntz(9))", srcTable));
    testConnection.createStatement().execute(
        String.format("create or replace table %s like %s", dstTable, srcTable));
    testConnection.createStatement().execute(
        String.format("insert into %s(c1,c2) values" +
                      "(1, '2018-05-12 12:34:56.123456789')," +
                      "(2, '2018-05-13 03:45:27.988')," +
                      "(3, '2018-05-14 07:12:34')," +
                      "(4, '2018-12-15 10:43:45.000000876')",
                      srcTable));

    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(
        testConnection, putConnection);
    StreamLoader loader =
        tdcb.setOnError("ABORT_STATEMENT")
            .setSchemaName(SCHEMA_NAME)
            .setTableName(dstTable)
            .setPreserveStageFile(true)
            .setColumns(Arrays.asList(
                "C1", "C2"
            ))
            .getStreamLoader();
    TestDataConfigBuilder.ResultListener listener = tdcb.getListener();
    listener.throwOnError = true;

    loader.start();

    ResultSet rs = testConnection.createStatement().executeQuery(
        String.format("select * from %s", srcTable));
    while (rs.next())
    {
      Object c1 = rs.getObject(1);
      Object c2 = rs.getObject(2);
      Object[] row = new Object[]
          {
              c1, c2
          };
      loader.submitRow(row);
    }
    loader.finish();

    rs = testConnection.createStatement().executeQuery(
        String.format("select * from %s minus select * from %s", dstTable, srcTable)
    );
    assertThat("No result", !rs.next());
  }

  @Test
  public void testLoadTime() throws Exception
  {
    String tableName = "LOADER_TIME_TEST";
    try
    {
      testConnection.createStatement().execute(String.format(
          "CREATE OR REPLACE TABLE %s ("
          + "ID int, "
          + "C1 time, C2 date)", tableName));

      TestDataConfigBuilder tdcb = new TestDataConfigBuilder(
          testConnection, putConnection);
      tdcb
          .setTableName(tableName)
          .setStartTransaction(true)
          .setTruncateTable(true)
          .setColumns(Arrays.asList(
              "ID", "C1", "C2"))
          .setKeys(Collections.singletonList(
              "ID"
          ));
      StreamLoader loader = tdcb.getStreamLoader();
      TestDataConfigBuilder.ResultListener listener = tdcb.getListener();
      loader.start();
      Time tm = new Time(3723000);
      Date dt = new Date();
      for (int i = 0; i < 10; i++)
      {
        Object[] row = new Object[]{i, tm, dt};
        loader.submitRow(row);
      }
      loader.finish();
      String errorMessage = "";
      if (listener.getErrorCount() > 0)
      {
        errorMessage = listener.getErrors().get(0).getException().toString();
      }
      assertThat(
          String.format("Error: %s", errorMessage),
          listener.getErrorCount(), equalTo(0));
      ResultSet rs = testConnection.createStatement().executeQuery(
          String.format(
              "SELECT c1, c2 FROM %s LIMIT 1", tableName));
      rs.next();
      Time rsTm = rs.getTime(1);
      Date rsDt = rs.getDate(2);
      assertThat("Time column didn't match", rsTm, equalTo(tm));

      Calendar cal = cutOffTimeFromDate(dt);
      long dtEpoch = cal.getTimeInMillis();
      long rsDtEpoch = rsDt.getTime();
      assertThat("Date column didn't match", rsDtEpoch, equalTo(dtEpoch));
    }
    finally
    {
      testConnection.createStatement().execute(String.format(
          "DROP TABLE IF EXISTS %s", tableName));
    }
  }

  private Calendar cutOffTimeFromDate(Date dt)
  {
    Calendar cal = Calendar.getInstance(); // locale-specific
    cal.setTimeZone(TimeZone.getTimeZone("UTC"));
    cal.setTime(dt);
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return cal;
  }


}
