/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.loader;

import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Random;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class LoaderErrorIT extends LoaderBase
{
  @Test
  public void testLoaderUpsertWithError() throws Exception
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

    Object[] upse = new Object[]
        {
            "10001-", "inserted", "something", "42-", new Date(), "{}"
        };
    loader.submitRow(upse);
    upse = new Object[]
        {
            10002, "inserted", "something", 43, new Date(), "{}"
        };
    loader.submitRow(upse);
    upse = new Object[]
        {
            45, "modified", "something", 46.1, new Date(), "{}"
        };
    loader.submitRow(upse);
    loader.finish();

    assertThat("processed", listener.processed.get(), equalTo(3));
    assertThat("counter", listener.counter.get(), equalTo(2));
    assertThat("submitted row", listener.getSubmittedRowCount(), equalTo(3));
    assertThat("updated/inserted", listener.updated.get(), equalTo(2));
    assertThat("error count", listener.getErrorCount(), equalTo(2));
    assertThat("error record count", listener.getErrorRecordCount(), equalTo(1));
    assertThat("Target table name is not correct", listener.getErrors().get(0)
        .getTarget(), equalTo(TARGET_TABLE_NAME));

    ResultSet rs = testConnection.createStatement().executeQuery(
        String.format("SELECT COUNT(*) AS N"
                      + " FROM \"%s\"", TARGET_TABLE_NAME));

    rs.next();
    int c = rs.getInt("N");
    assertThat("N is not correct", c, equalTo(10001));

    rs = testConnection.createStatement().executeQuery(
        String.format("SELECT C1 AS N"
                      + " FROM \"%s\" WHERE ID=45", TARGET_TABLE_NAME));

    rs.next();
    assertThat("N is not correct", rs.getString("N"), equalTo("modified"));
  }

  @Test
  public void testLoaderUpsertWithErrorAndRollback() throws Exception
  {
    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(
        testConnection, putConnection);
    tdcb.populate();

    PreparedStatement pstmt = testConnection.prepareStatement(
        String.format("INSERT INTO \"%s\"(ID,C1,C2,C3,C4,C5)"
                      + " SELECT column1, column2, column3, column4,"
                      + " column5, parse_json(column6)"
                      + " FROM VALUES(?,?,?,?,?,?)", TARGET_TABLE_NAME));
    pstmt.setInt(1, 10001);
    pstmt.setString(2, "inserted\\,");
    pstmt.setString(3, "something");
    pstmt.setDouble(4, 0x4.11_33p2);
    pstmt.setDate(5, new java.sql.Date(new Date().getTime()));
    pstmt.setObject(6, "{}");
    pstmt.execute();
    testConnection.commit();

    TestDataConfigBuilder tdcbUpsert = new TestDataConfigBuilder(
        testConnection, putConnection);
    tdcbUpsert
        .setOperation(Operation.UPSERT)
        .setTruncateTable(false)
        .setStartTransaction(true)
        .setPreserveStageFile(true)
        .setColumns(Arrays.asList(
            "ID", "C1", "C2", "C3", "C4", "C5"
        ))
        .setKeys(Collections.singletonList(
            "ID"
        ));
    StreamLoader loader = tdcbUpsert.getStreamLoader();
    TestDataConfigBuilder.ResultListener listener = tdcbUpsert.getListener();
    listener.throwOnError = true; // should trigger rollback
    loader.start();
    try
    {

      Object[] noerr = new Object[]
          {
              "10001", "inserted", "something", "42", new Date(), "{}"
          };
      loader.submitRow(noerr);

      Object[] err = new Object[]
          {
              "10002-", "inserted", "something", "42-", new Date(), "{}"
          };
      loader.submitRow(err);

      loader.finish();

      fail("Test must raise Loader.DataError exception");
    }
    catch (Loader.DataError e)
    {
      // we are good
      assertThat("error message",
                 e.getMessage(), allOf(
              containsString("10002-"),
              containsString("not recognized")));
    }

    assertThat("processed", listener.processed.get(), equalTo(0));
    assertThat("submitted row", listener.getSubmittedRowCount(), equalTo(2));
    assertThat("updated/inserted", listener.updated.get(), equalTo(0));
    assertThat("error count", listener.getErrorCount(), equalTo(2));
    assertThat("error record count", listener.getErrorRecordCount(), equalTo(1));

    ResultSet rs = testConnection.createStatement().executeQuery(
        String.format("SELECT COUNT(*) AS N FROM \"%s\"", TARGET_TABLE_NAME));
    rs.next();
    assertThat("N", rs.getInt("N"), equalTo(10001));

    rs = testConnection.createStatement().executeQuery(
        String.format("SELECT C3 FROM \"%s\" WHERE id=10001", TARGET_TABLE_NAME));
    rs.next();
    assertThat("C3. No commit should happen",
               Double.toHexString((rs.getDouble("C3"))),
               equalTo("0x1.044cc0000225cp4"));
  }

  @Test
  public void testInjectBadStagedFileInsert() throws Exception
  {
    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(
        testConnection, putConnection);
    TestDataConfigBuilder.ResultListener listener = tdcb.getListener();
    StreamLoader loader = tdcb.setOnError("ABORT_STATEMENT").getStreamLoader();
    listener.throwOnError = true;
    int numberOfRows = 1000;
    loader.setProperty(LoaderProperty.testRemoteBadCSV, true);
    loader.setProperty(LoaderProperty.startTransaction, true);
    loader.start();
    Random rnd = new Random();

    // generates a new data set and ingest
    for (int i = 0; i < numberOfRows; i++)
    {
      final String json = "{\"key\":" + rnd.nextInt() + ","
                          + "\"bar\":" + i + "}";
      Object[] row = new Object[]
          {
              i, "foo_" + i, rnd.nextInt() / 3, new Date(),
              json
          };
      loader.submitRow(row);
    }
    try
    {
      loader.finish();
      fail("Should raise and error");
    }
    catch (Loader.DataError ex)
    {
      assertThat("Loader.DataError is raised", true);
    }
  }

  @Test
  public void testLoaderInsertAbortStatement() throws Exception
  {
    TestDataConfigBuilder tdcb = new TestDataConfigBuilder(
        testConnection, putConnection);
    TestDataConfigBuilder.ResultListener listener = tdcb.getListener();
    StreamLoader loader = tdcb.setOnError("ABORT_STATEMENT").getStreamLoader();
    listener.throwOnError = true;

    loader.start();
    Random rnd = new Random();

    // generates a new data set and ingest
    for (int i = 0; i < 10; i++)
    {
      final String json = "{\"key\":" + rnd.nextInt() + ","
                          + "\"bar\":" + i + "}";
      Object v = rnd.nextInt() / 3;
      if (i == 7)
      {
        v = "INVALID_INTEGER";
      }
      Object[] row = new Object[]
          {
              i, "foo_" + i, v, new Date(), json
          };
      loader.submitRow(row);
    }
    try
    {
      loader.finish();
      fail("should raise an exception");
    }
    catch (Loader.DataError ex)
    {
      assertThat(ex.toString(), containsString("INVALID_INTEGER"));
    }
  }

  @Test
  public void testExecuteBeforeAfterSQLError() throws Exception
  {
    TestDataConfigBuilder tdcbBefore = new TestDataConfigBuilder(
        testConnection, putConnection);
    StreamLoader loaderBefore = tdcbBefore.setOnError("ABORT_STATEMENT").getStreamLoader();
    loaderBefore.setProperty(
        LoaderProperty.executeBefore, "SELECT * FROOOOOM TBL");
    loaderBefore.start();
    try
    {
      loaderBefore.finish();
      fail("SQL Error should be raised.");
    }
    catch (Loader.ConnectionError e)
    {
      assertThat(e.getCause(), instanceOf(SQLException.class));
    }

    TestDataConfigBuilder tdcbAfter = new TestDataConfigBuilder(
        testConnection, putConnection);
    StreamLoader loaderAfter = tdcbAfter.setOnError("ABORT_STATEMENT").getStreamLoader();
    loaderAfter.setProperty(
        LoaderProperty.executeBefore, "select current_version()");
    loaderAfter.setProperty(
        LoaderProperty.executeAfter, "SELECT * FROM TBBBBBBL");
    loaderAfter.start();
    try
    {
      loaderAfter.finish();
      fail("SQL Error should be raised.");
    }
    catch (Loader.ConnectionError e)
    {
      assertThat(e.getCause(), instanceOf(SQLException.class));
    }
  }
}
