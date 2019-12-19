import org.hsqldb.Statement;
import org.hsqldb.jdbc.JDBCStatement;

import java.sql.*;
import java.util.ArrayList;

public class Test{



    public static void main(String args[]){
        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver" );
        } catch (Exception e) {
            System.err.println("ERROR: failed to load HSQLDB JDBC driver.");
            e.printStackTrace();
            return;
        }
        try {
            Connection c = DriverManager.getConnection("jdbc:hsqldb:file:data\\test", "SA", "");
            System.out.println("Connected");
            ResultSet resultSet;
            JDBCStatement statement = (JDBCStatement) c.createStatement();
            resultSet= statement.executeQuery("EXPLAIN JSON PLAN FOR SELECT * FROM INVOICE as I join CUSTOMER as C on I.CUSTOMERID = C.ID WHERE I.TOTAL=50 AND I.ID IN (SELECT INVOICEID FROM ITEM as IT join PRODUCT as P on IT.PRODUCTID = P.ID where COST > (SELECT TOP 1 ID FROM CUSTOMER))");
            //resultSet = statement.getExecutionPlan("SELECT MAX(I.ID),AVG(I.TOTAL) FROM INVOICE as I join CUSTOMER as C on I.CUSTOMERID = C.ID WHERE I.TOTAL=50 AND I.ID IN (SELECT INVOICEID FROM ITEM as IT join PRODUCT as P on IT.PRODUCTID = P.ID where COST > (SELECT TOP 1 ID FROM CUSTOMER))");
            while (resultSet.next()){
                System.out.println(resultSet.getObject(1).toString());
            }

            c.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}