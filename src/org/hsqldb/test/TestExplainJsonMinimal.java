package org.hsqldb.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.TestCase;

/**
 * Test cases for HSQL EXPLAIN JSON MINIMAL FOR statement, based on TestGroupByHaving and TestSubselect.
 *
 * @author Roberto Ripamonti
 */

public class TestExplainJsonMinimal extends TestCase {

    //------------------------------------------------------------
    // Class variables
    //------------------------------------------------------------
    private static final String databaseDriver   = "org.hsqldb.jdbc.JDBCDriver";
    private static final String databaseURL      = "jdbc:hsqldb:mem:.";
    private static final String databaseUser     = "sa";
    private static final String databasePassword = "";

    //------------------------------------------------------------
    // Instance variables
    //------------------------------------------------------------
    private Connection conn;
    private Statement  stmt;

    //------------------------------------------------------------
    // Constructors
    //------------------------------------------------------------

    /**
     * Constructs a new SubselectTest.
     */
    public TestExplainJsonMinimal(String s) {
        super(s);
    }

    //------------------------------------------------------------
    // Class methods
    //------------------------------------------------------------
    protected static Connection getJDBCConnection() throws SQLException {
        return DriverManager.getConnection(databaseURL, databaseUser,
                databasePassword);
    }

    protected void setUp() throws Exception {

        super.setUp();

        if (conn != null) {
            return;
        }

        Class.forName(databaseDriver);

        conn = getJDBCConnection();
        stmt = conn.createStatement();

        // I decided not the use the "IF EXISTS" clause since it is not a
        // SQL standard.
        try {

//            stmt.execute("drop table employee");
            stmt.execute("drop table employee if exists");
        } catch (Exception x) {}

        stmt.execute("create table employee(id int, "
                + "firstname VARCHAR(50), " + "lastname VARCHAR(50), "
                + "salary decimal(10, 2), " + "superior_id int, "
                + "CONSTRAINT PK_employee PRIMARY KEY (id), "
                + "CONSTRAINT FK_superior FOREIGN KEY (superior_id) "
                + "REFERENCES employee(ID))");
        addEmployee(1, "Mike", "Smith", 160000, -1);
        addEmployee(2, "Mary", "Smith", 140000, -1);

        // Employee under Mike
        addEmployee(10, "Joe", "Divis", 50000, 1);
        addEmployee(11, "Peter", "Mason", 45000, 1);
        addEmployee(12, "Steve", "Johnson", 40000, 1);
        addEmployee(13, "Jim", "Hood", 35000, 1);

        // Employee under Mike
        addEmployee(20, "Jennifer", "Divis", 60000, 2);
        addEmployee(21, "Helen", "Mason", 50000, 2);
        addEmployee(22, "Daisy", "Johnson", 40000, 2);
        addEmployee(23, "Barbara", "Hood", 30000, 2);

        stmt.execute("drop table colors if exists; "
                + "drop table sizes if exists; ");
        stmt.execute("create table colors(id int, val varchar(10)); ");
        stmt.execute("insert into colors values(1,'red'); "
                + "insert into colors values(2,'green'); "
                + "insert into colors values(3,'orange'); "
                + "insert into colors values(4,'indigo'); ");
        stmt.execute("create table sizes(id int, val varchar(10)); ");
        stmt.execute("insert into sizes values(1,'small'); "
                + "insert into sizes values(2,'medium'); "
                + "insert into sizes values(3,'large'); "
                + "insert into sizes values(4,'odd'); ");
    }

    protected void tearDown() throws Exception {

        // I decided not the use the "IF EXISTS" clause since it is not a
        // SQL standard.
        try {

//            stmt.execute("drop table employee");
            stmt.execute("drop table employee if exists; "+"drop table colors if exists; "
                    + "drop table sizes if exists; ");
        } catch (Exception x) {}

        if (stmt != null) {
            stmt.close();

            stmt = null;
        }

        if (conn != null) {
            conn.close();

            conn = null;
        }

        super.tearDown();

    }

    private void addEmployee(int id, String firstName, String lastName,
                             double salary, int superiorId) throws Exception {

        stmt.execute("insert into employee values(" + id + ", '" + firstName
                + "', '" + lastName + "', " + salary + ", "
                + (superiorId <= 0 ? "null"
                : ("" + superiorId)) + ")");
    }

    /**
     * Test EXPLAIN JSON MINIMAL FOR statement for a simple select without where conditions
     */
    public void testSimpleSelect() throws SQLException {

        String sql = "EXPLAIN JSON MINIMAL FOR " + "select firstName from employee;";
        Object expected = "{\"SELECT\":{\"QUERYSPECIFICATION\":{\"COLUMNS\":[{\"EXPRESSION_COLUMN\":{\"VALUE\":\"PUBLIC.EMPLOYEE.FIRSTNAME\"}}],\"RANGEVARIABLES\":[{\"JOINTYPE\":\"INNER\",\"TABLE\":\"PUBLIC.EMPLOYEE\",\"CONDITIONS\":[[]]}],\"QUERYCONDITION\":{}}}}";

        compareResults(sql, expected, "00000");
    }

    /**
     * Test EXPLAIN JSON MINIMAL FOR statement for a simple select without where conditions but ordered
     */
    public void testSimpleSelectOrderBy() throws SQLException {

        String sql = "EXPLAIN JSON MINIMAL FOR " + "select firstName from employee order by salary;";
        Object expected = "{\"SELECT\":{\"QUERYSPECIFICATION\":{\"COLUMNS\":[{\"EXPRESSION_COLUMN\":{\"VALUE\":\"PUBLIC.EMPLOYEE.FIRSTNAME\"}}],\"RANGEVARIABLES\":[{\"JOINTYPE\":\"INNER\",\"TABLE\":\"PUBLIC.EMPLOYEE\",\"CONDITIONS\":[[]]}],\"QUERYCONDITION\":{},\"ORDERBY\":{\"EXPRESSIONS\":[{\"ORDERBY\":{\"EXPRESSION_COLUMN\":{\"VALUE\":\"PUBLIC.EMPLOYEE.SALARY\"}}}]}}}}";

        compareResults(sql, expected, "00000");
    }

    /**
     * Test EXPLAIN JSON MINIMAL FOR statement for a simple select with simple where conditions
     */
    public void testSimpleSelectWithWhere() throws SQLException {

        String sql = "EXPLAIN JSON MINIMAL FOR " + "select firstName from employee where id=20;";
        Object expected = "{\"SELECT\":{\"QUERYSPECIFICATION\":{\"COLUMNS\":[{\"EXPRESSION_COLUMN\":{\"VALUE\":\"PUBLIC.EMPLOYEE.FIRSTNAME\"}}],\"RANGEVARIABLES\":[{\"JOINTYPE\":\"INNER\",\"TABLE\":\"PUBLIC.EMPLOYEE\",\"CONDITIONS\":[[{\"EXPRESSION_LOGICAL\":{\"OPTYPE\":\"EQUAL\",\"ARG_LEFT\":{\"EXPRESSION_COLUMN\":{\"VALUE\":\"PUBLIC.EMPLOYEE.ID\"}},\"ARG_RIGHT\":{\"EXPRESSION_VALUE\":{\"VALUE\":\"20\",\"TYPE\":\"INTEGER\"}}}}]]}],\"QUERYCONDITION\":{}}}}";

        compareResults(sql, expected, "00000");
    }

    /**
     * Test EXPLAIN JSON MINIMAL FOR statement for a select with an aggregate and a simple where conditions
     */
    public void testAggSelectWithWhere() throws SQLException {

        String sql = "EXPLAIN JSON MINIMAL FOR " + "select avg(salary) from employee where superior_id=2;";
        Object expected = "{\"SELECT\":{\"QUERYSPECIFICATION\":{\"COLUMNS\":[{\"EXPRESSION_AGGREGATE\":{\"OPTYPE\":\"AVG\",\"ARG\":{\"EXPRESSION_COLUMN\":{\"VALUE\":\"PUBLIC.EMPLOYEE.SALARY\"}}}}],\"RANGEVARIABLES\":[{\"JOINTYPE\":\"INNER\",\"TABLE\":\"PUBLIC.EMPLOYEE\",\"CONDITIONS\":[[{\"EXPRESSION_LOGICAL\":{\"OPTYPE\":\"EQUAL\",\"ARG_LEFT\":{\"EXPRESSION_COLUMN\":{\"VALUE\":\"PUBLIC.EMPLOYEE.SUPERIOR_ID\"}},\"ARG_RIGHT\":{\"EXPRESSION_VALUE\":{\"VALUE\":\"2\",\"TYPE\":\"INTEGER\"}}}}]]}],\"QUERYCONDITION\":{}}}}";

        compareResults(sql, expected, "00000");
    }

    /**
     * Test EXPLAIN JSON MINIMAL FOR statement for a select with an aggregate, a simple where conditions and a group by
     */
    public void testGroupBySelectWithWhere() throws SQLException {

        String sql = "EXPLAIN JSON MINIMAL FOR " + "select superior_id, avg(salary) from employee where superior_id>0 group by superior_id;";
        Object expected = "{\"SELECT\":{\"QUERYSPECIFICATION\":{\"COLUMNS\":[{\"EXPRESSION_COLUMN\":{\"VALUE\":\"PUBLIC.EMPLOYEE.SUPERIOR_ID\"}},{\"EXPRESSION_AGGREGATE\":{\"OPTYPE\":\"AVG\",\"ARG\":{\"EXPRESSION_COLUMN\":{\"VALUE\":\"PUBLIC.EMPLOYEE.SALARY\"}}}}],\"RANGEVARIABLES\":[{\"JOINTYPE\":\"INNER\",\"TABLE\":\"PUBLIC.EMPLOYEE\",\"CONDITIONS\":[[{\"EXPRESSION_LOGICAL\":{\"OPTYPE\":\"GREATER\",\"ARG_LEFT\":{\"EXPRESSION_COLUMN\":{\"VALUE\":\"PUBLIC.EMPLOYEE.SUPERIOR_ID\"}},\"ARG_RIGHT\":{\"EXPRESSION_VALUE\":{\"VALUE\":\"0\",\"TYPE\":\"INTEGER\"}}}}]]}],\"QUERYCONDITION\":{},\"GROUPCOLUMNS\":[{\"EXPRESSION_COLUMN\":{\"VALUE\":\"PUBLIC.EMPLOYEE.SUPERIOR_ID\"}}]}}}";

        compareResults(sql, expected, "00000");
    }

    /**
     * Test EXPLAIN JSON MINIMAL FOR statement for a select with an aggregate, a group by, an having cond and an order by clause
     */
    public void testAggGroupBySelectWithHavingAndOrderBy() throws SQLException {

        String sql = "EXPLAIN JSON MINIMAL FOR " + "select avg(salary), max(id) from employee group by superior_id having superior_id is not null order by superior_id;";
        Object expected = "{\"SELECT\":{\"QUERYSPECIFICATION\":{\"COLUMNS\":[{\"EXPRESSION_AGGREGATE\":{\"OPTYPE\":\"AVG\",\"ARG\":{\"EXPRESSION_COLUMN\":{\"VALUE\":\"PUBLIC.EMPLOYEE.SALARY\"}}}},{\"EXPRESSION_AGGREGATE\":{\"OPTYPE\":\"MAX\",\"ARG\":{\"EXPRESSION_COLUMN\":{\"VALUE\":\"PUBLIC.EMPLOYEE.ID\"}}}}],\"RANGEVARIABLES\":[{\"JOINTYPE\":\"INNER\",\"TABLE\":\"PUBLIC.EMPLOYEE\",\"CONDITIONS\":[[]]}],\"QUERYCONDITION\":{},\"GROUPCOLUMNS\":[{\"EXPRESSION_COLUMN\":{\"VALUE\":\"PUBLIC.EMPLOYEE.SUPERIOR_ID\"}}],\"HAVINGCONDITION\":{\"EXPRESSION_LOGICAL\":{\"OPTYPE\":\"IS_NOT_NULL\",\"ARG_LEFT\":{\"EXPRESSION_COLUMN\":{\"VALUE\":\"PUBLIC.EMPLOYEE.SUPERIOR_ID\"}}}},\"ORDERBY\":{\"EXPRESSIONS\":[{\"ORDERBY\":{\"EXPRESSION_COLUMN\":{\"VALUE\":\"PUBLIC.EMPLOYEE.SUPERIOR_ID\"}}}]}}}}";

        compareResults(sql, expected, "00000");
    }

    /**
     * Test EXPLAIN JSON MINIMAL FOR statement for a select with a join
     */
    public void testSelectJoin() throws SQLException {

        String sql = "EXPLAIN JSON MINIMAL FOR " + "select * from colors join sizes on colors.id=sizes.id;";
        Object expected = "{\"SELECT\":{\"QUERYSPECIFICATION\":{\"COLUMNS\":[{\"EXPRESSION_COLUMN\":{\"VALUE\":\"PUBLIC.COLORS.ID\"}},{\"EXPRESSION_COLUMN\":{\"VALUE\":\"PUBLIC.COLORS.VAL\"}},{\"EXPRESSION_COLUMN\":{\"VALUE\":\"PUBLIC.SIZES.ID\"}},{\"EXPRESSION_COLUMN\":{\"VALUE\":\"PUBLIC.SIZES.VAL\"}}],\"RANGEVARIABLES\":[{\"JOINTYPE\":\"INNER\",\"TABLE\":\"PUBLIC.COLORS\",\"CONDITIONS\":[[]]},{\"JOINTYPE\":\"INNER\",\"TABLE\":\"PUBLIC.SIZES\",\"CONDITIONS\":[[{\"EXPRESSION_LOGICAL\":{\"OPTYPE\":\"EQUAL\",\"ARG_LEFT\":{\"EXPRESSION_COLUMN\":{\"VALUE\":\"PUBLIC.SIZES.ID\"}},\"ARG_RIGHT\":{\"EXPRESSION_COLUMN\":{\"VALUE\":\"PUBLIC.COLORS.ID\"}}}}]]}],\"QUERYCONDITION\":{}}}}";

        compareResults(sql, expected, "00000");
    }
    //------------------------------------------------------------
    // Helper methods
    //------------------------------------------------------------
    private void compareResults(String sql, Object row,
                                String sqlState) throws SQLException {

        ResultSet rs = null;

        try {
            rs = stmt.executeQuery(sql);

            assertTrue("Statement <" + sql + "> \nexpecting error code: "
                    + sqlState, ("00000".equals(sqlState)));
        } catch (SQLException sqlx) {
            if (!sqlx.getSQLState().equals(sqlState)) {
                sqlx.printStackTrace();
            }

            assertTrue("Statement <" + sql + "> \nthrows wrong error code: "
                    + sqlx.getErrorCode() + " expecting error code: "
                    + sqlState, (sqlx.getSQLState().equals(sqlState)));

            return;
        }

        int rowCount = 0;
        int colCount = 1;

        while (rs.next()) {
            assertTrue("Statement <" + sql + "> \nreturned too many rows.",
                    (rowCount < 1));

            for (int col = 1, i = 0; i < colCount; i++, col++) {
                Object result   = null;
                Object expected = row;

                if (expected == null) {
                    result = rs.getString(col);
                    result = rs.wasNull() ? null
                            : result;
                } else if (expected instanceof String) {
                    result = rs.getString(col);
                } else if (expected instanceof Double) {
                    result = Double.valueOf(rs.getString(col));
                } else if (expected instanceof Integer) {
                    result = Integer.valueOf(rs.getInt(col));
                }

                assertEquals("Statement <" + sql
                                + "> \nreturned wrong value.", row,
                        result);
            }

            rowCount++;
        }

        assertEquals("Statement <" + sql
                        + "> \nreturned wrong number of rows.", 1,
                rowCount);
    }
}
