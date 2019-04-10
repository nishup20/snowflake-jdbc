/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.loader;

import org.junit.Ignore;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

/**
 * Loader IT
 */
public class LoaderIT extends LoaderBase
{
  /**
   * This is to run performance tests. Set the environment variables
   * and run this by VisualVM, YourKit, or any profiler.
   * <p>
   * SNOWFLAKE_TEST_ACCOUNT=testaccount
   * SNOWFLAKE_TEST_USER=testuser
   * SNOWFLAKE_TEST_PASSWORD=testpassword
   * SNOWFLAKE_TEST_HOST=testaccount.snowflakecomputing.com
   * SNOWFLAKE_TEST_PORT=443
   * SNOWFLAKE_TEST_DATABASE=testdb
   * SNOWFLAKE_TEST_SCHEMA=public
   * SNOWFLAKE_TEST_WAREHOUSE=testwh
   * SNOWFLAKE_TEST_ROLE=sysadmin
   * SNOWFLAKE_TEST_PROTOCOL=https
   *
   * @throws Exception raises an exception if any error occurs.
   */
  @Ignore("Performance test")
  @Test
  public void testLoaderLargeInsert() throws Exception
  {
    new TestDataConfigBuilder(testConnection, putConnection)
        .setDatabaseName("INFORMATICA_DB")
        .setCompressDataBeforePut(false)
        .setCompressFileByPut(true)
        .setNumberOfRows(10000000)
        .setCsvFileSize(100000000L)
        .setCsvFileBucketSize(64)
        .populate();
  }

  @Test
  public void testLoaderInsert() throws Exception
  {
    // mostly just populate test data but with delay injection to test
    // PUT retry
    new TestDataConfigBuilder(testConnection, putConnection)
        .setTestMode(true).populate();
  }

  @Test
  public void testLoaderDelete() throws Exception
  {
    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(
        testConnection, putConnection);
    tdcb.populate();

    TestDataConfigBuilder tdcbDelete = new TestDataConfigBuilder(
        testConnection, putConnection
    );
    tdcbDelete
        .setOperation(Operation.DELETE)
        .setTruncateTable(false)
        .setColumns(Arrays.asList(
            "ID", "C1"
        ))
        .setKeys(Arrays.asList(
            "ID", "C1"
        ));
    StreamLoader loader = tdcbDelete.getStreamLoader();
    TestDataConfigBuilder.ResultListener listener = tdcbDelete.getListener();
    loader.start();

    Object[] del = new Object[]
        {
            42, "foo_42" // deleted
        };
    loader.submitRow(del);

    del = new Object[]
        {
            41, "blah" // ignored, and should not raise any error/warning
        };
    loader.submitRow(del);
    loader.finish();

    assertThat("error count", listener.getErrorCount(), equalTo(0));
    assertThat("error record count",
               listener.getErrorRecordCount(), equalTo(0));
    assertThat("submitted row count",
               listener.getSubmittedRowCount(), equalTo(2));
    assertThat("processed", listener.processed.get(), equalTo(1));
    assertThat("deleted rows", listener.deleted.get(), equalTo(1));
  }

  @Test
  public void testLoaderModify() throws Exception
  {
    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(
        testConnection, putConnection);
    tdcb.populate();

    TestDataConfigBuilder tdcbModify = new TestDataConfigBuilder(
        testConnection, putConnection);
    tdcbModify
        .setOperation(Operation.MODIFY)
        .setTruncateTable(false)
        .setColumns(Arrays.asList(
            "ID", "C1", "C2", "C3", "C4", "C5"
        ))
        .setKeys(Collections.singletonList(
            "ID"
        ));
    StreamLoader loader = tdcbModify.getStreamLoader();
    TestDataConfigBuilder.ResultListener listener = tdcbModify.getListener();
    loader.start();

    Object[] mod = new Object[]
        {
            41, "modified", "some\nthi\"ng\\", 41.6, new Date(), "{}"
        };
    loader.submitRow(mod);
    mod = new Object[]
        {
            40, "modified", "\"something,", 40.2, new Date(), "{}"
        };
    loader.submitRow(mod);
    loader.finish();

    assertThat("processed", listener.processed.get(), equalTo(2));
    assertThat("submitted row", listener.getSubmittedRowCount(), equalTo(2));
    assertThat("updated", listener.updated.get(), equalTo(2));
    assertThat("error count", listener.getErrorCount(), equalTo(0));
    assertThat("error record count", listener.getErrorRecordCount(), equalTo(0));

    // Test deletion
    ResultSet rs = testConnection.createStatement().executeQuery(
        String.format("SELECT COUNT(*) AS N"
                      + " FROM \"%s\"", TARGET_TABLE_NAME));
    rs.next();
    assertThat("count is not correct", rs.getInt("N"), equalTo(10000));

    rs = testConnection.createStatement().executeQuery(
        String.format("SELECT C1 AS N"
                      + " FROM \"%s\" WHERE ID=40", TARGET_TABLE_NAME));

    rs.next();
    assertThat("status is not correct", rs.getString("N"), equalTo("modified"));

    rs = testConnection.createStatement().executeQuery(
        String.format("SELECT C1, C2"
                      + " FROM \"%s\" WHERE ID=41", TARGET_TABLE_NAME));
    rs.next();
    assertThat("C1 is not correct",
               rs.getString("C1"), equalTo("modified"));
    assertThat("C2 is not correct",
               rs.getString("C2"), equalTo("some\nthi\"ng\\"));
  }

  @Test
  public void testLoaderModifyWithOneMatchOneNot() throws Exception
  {
    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(
        testConnection, putConnection);
    tdcb.populate();

    TestDataConfigBuilder tdcbModify = new TestDataConfigBuilder(
        testConnection, putConnection
    );
    tdcbModify
        .setTruncateTable(false)
        .setOperation(Operation.MODIFY)
        .setColumns(Arrays.asList(
            "ID", "C1", "C2", "C3", "C4", "C5"
        ))
        .setKeys(Collections.singletonList(
            "ID"
        ));
    StreamLoader loader = tdcbModify.getStreamLoader();
    TestDataConfigBuilder.ResultListener listener = tdcbModify.getListener();
    loader.start();

    Object[] mod = new Object[]
        {
            20000, "modified", "some\nthi\"ng\\", 41.6, new Date(), "{}"
        };
    loader.submitRow(mod);
    mod = new Object[]
        {
            45, "modified", "\"something2,", 40.2, new Date(), "{}"
        };
    loader.submitRow(mod);
    loader.finish();

    assertThat("processed", listener.processed.get(), equalTo(1));
    assertThat("submitted row", listener.getSubmittedRowCount(), equalTo(2));
    assertThat("updated", listener.updated.get(), equalTo(1));
    assertThat("error count", listener.getErrorCount(), equalTo(0));
    assertThat("error record count", listener.getErrorRecordCount(), equalTo(0));

    // Test deletion
    ResultSet rs = testConnection.createStatement().executeQuery(
        String.format("SELECT COUNT(*) AS N"
                      + " FROM \"%s\"", TARGET_TABLE_NAME));
    rs.next();
    assertThat("count is not correct", rs.getInt("N"), equalTo(10000));

    rs = testConnection.createStatement().executeQuery(
        String.format("SELECT C1, C2"
                      + " FROM \"%s\" WHERE ID=45", TARGET_TABLE_NAME));
    rs.next();
    assertThat("C1 is not correct",
               rs.getString("C1"), equalTo("modified"));
    assertThat("C2 is not correct",
               rs.getString("C2"), equalTo("\"something2,"));
  }

  @Test
  public void testLoaderUpsert() throws Exception
  {
    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(
        testConnection, putConnection);
    tdcb.populate();

    TestDataConfigBuilder tdcbUpsert = new TestDataConfigBuilder(
        testConnection, putConnection);
    tdcbUpsert
        .setOperation(Operation.UPSERT)
        .setTruncateTable(false)
        .setColumns(Arrays.asList(
            "ID", "C1", "C2", "C3", "C4", "C5"
        ))
        .setKeys(Collections.singletonList(
            "ID"
        ));
    StreamLoader loader = tdcbUpsert.getStreamLoader();
    TestDataConfigBuilder.ResultListener listener = tdcbUpsert.getListener();
    loader.start();

    Date d = new Date();

    Object[] ups = new Object[]
        {
            10001, "inserted\\,", "something", 0x4.11_33p2, d, "{}"
        };
    loader.submitRow(ups);
    ups = new Object[]
        {
            39, "modified", "something", 40.1, d, "{}"
        };
    loader.submitRow(ups);
    loader.finish();

    assertThat("processed", listener.processed.get(), equalTo(2));
    assertThat("submitted row", listener.getSubmittedRowCount(), equalTo(2));
    assertThat("updated/inserted", listener.updated.get(), equalTo(2));
    assertThat("error count", listener.getErrorCount(), equalTo(0));
    assertThat("error record count", listener.getErrorRecordCount(), equalTo(0));

    ResultSet rs = testConnection.createStatement().executeQuery(
        String.format("SELECT C1, C4, C3"
                      + " FROM \"%s\" WHERE ID=10001", TARGET_TABLE_NAME));

    rs.next();
    assertThat("C1 is not correct", rs.getString("C1"), equalTo("inserted\\,"));

    long l = rs.getTimestamp("C4").getTime();
    assertThat("C4 is not correct", l, equalTo(d.getTime()));
    assertThat("C3 is not correct", Double.toHexString((rs.getDouble("C3"))),
               equalTo("0x1.044cc0000225cp4"));

    rs = testConnection.createStatement().executeQuery(
        String.format("SELECT C1 AS N"
                      + " FROM \"%s\" WHERE ID=39", TARGET_TABLE_NAME));

    rs.next();
    assertThat("N is not correct", rs.getString("N"), equalTo("modified"));
  }

  @Test
  public void testEmptyFieldAsEmpty() throws Exception
  {
    _testEmptyFieldAsEmpty(true);
    _testEmptyFieldAsEmpty(false);
  }

  private void _testEmptyFieldAsEmpty(boolean copyEmptyFieldAsEmpty) throws Exception
  {
    String tableName = "LOADER_EMPTY_FIELD_AS_NULL";
    try
    {
      testConnection.createStatement().execute(String.format(
          "CREATE OR REPLACE TABLE %s ("
          + "ID int, "
          + "C1 string, C2 string)", tableName));

      TestDataConfigBuilder tdcb = new TestDataConfigBuilder(
          testConnection, putConnection
      );
      tdcb
          .setOperation(Operation.INSERT)
          .setStartTransaction(true)
          .setTruncateTable(true)
          .setTableName(tableName)
          .setCopyEmptyFieldAsEmpty(copyEmptyFieldAsEmpty)
          .setColumns(Arrays.asList(
              "ID", "C1", "C2"
          ));
      StreamLoader loader = tdcb.getStreamLoader();
      TestDataConfigBuilder.ResultListener listener = tdcb.getListener();
      loader.start();

      // insert null
      loader.submitRow(new Object[]
                           {
                               1, null, ""
                           });
      loader.finish();
      int submitted = listener.getSubmittedRowCount();
      assertThat("submitted rows", submitted, equalTo(1));

      ResultSet rs = testConnection.createStatement().executeQuery(
          String.format("SELECT C1, C2 FROM %s", tableName));

      rs.next();
      String c1 = rs.getString(1); // null
      String c2 = rs.getString(2); // empty
      if (!copyEmptyFieldAsEmpty)
      {
        // COPY ... EMPTY_FIELD_AS_NULL = TRUE /* default */
        assertThat(c1, is(nullValue()));  // null => null
        assertThat(c2, equalTo("")); // empty => empty
      }
      else
      {
        // COPY EMPTY_FIELD_AS_NULL = FALSE
        assertThat(c1, equalTo("")); // null => empty
        assertThat(c2, equalTo("")); // empty => empty
      }
      rs.close();
    }
    finally
    {
      testConnection.createStatement().execute(String.format(
          "DROP TABLE IF EXISTS %s", tableName));
    }
  }

  /**
   * Test a target table name including spaces.
   *
   * @throws Exception raises if any error occurs
   */
  @Test
  public void testSpacesInColumnTable() throws Exception
  {
    String targetTableName = "Load Test Spaces In Columns";

    // create table with spaces in column names
    testConnection.createStatement().execute(String.format(
        "CREATE OR REPLACE TABLE \"%s\" ("
        + "ID int, "
        + "\"Column 1\" varchar(255))", targetTableName));

    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(
        testConnection, putConnection);
    tdcb
        .setTableName(targetTableName)
        .setColumns(Arrays.asList(
            "ID", "Column 1"
        ));

    StreamLoader loader = tdcb.getStreamLoader();
    loader.start();

    for (int i = 0; i < 5; ++i)
    {
      Object[] row = new Object[]{
          i, "foo_" + i
      };
      loader.submitRow(row);
    }
    loader.finish();

    ResultSet rs = testConnection.createStatement().executeQuery(
        String.format("SELECT * FROM \"%s\" ORDER BY \"Column 1\"",
                      targetTableName));

    rs.next();
    assertThat("The first id", rs.getInt(1), equalTo(0));
    assertThat("The first str", rs.getString(2), equalTo("foo_0"));
  }
}
