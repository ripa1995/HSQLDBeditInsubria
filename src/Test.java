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
            //resultSet= statement.executeQuery("explain json minimal for SELECT C.ID FROM INVOICE as I join CUSTOMER as C on I.CUSTOMERID = C.ID WHERE I.TOTAL>200");
            //resultSet= statement.executeQuery("explain json minimal for SELECT I.ID, I.TOTAL FROM INVOICE as I");
            //resultSet= statement.executeQuery("explain json minimal for SELECT I.ID, I.TOTAL FROM INVOICE as I WHERE I.TOTAL>100");
            //resultSet= statement.executeQuery("explain json minimal for SELECT I.CUSTOMERID, MAX(I.TOTAL) FROM INVOICE as I group by (I.CUSTOMERID) having I.CustomerID is not null");
            resultSet= statement.executeQuery("explain json minimal for SELECT AVG(TOTAL) as average,MAX(I.ID) as ID_IN,C.ID FROM INVOICE as I left join CUSTOMER as C on CUSTOMERID = C.ID WHERE TOTAL>200 GROUP BY C.ID having AVG(TOTAL)>200");
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