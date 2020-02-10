import org.hsqldb.Statement;
import org.hsqldb.jdbc.JDBCStatement;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.stream.Stream;

public class Test{


    private static JDBCStatement statement;

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
            statement = (JDBCStatement) c.createStatement();
            importAdultDataset();


            resultSet= statement.executeQuery("explain json minimal for SELECT AVG(TOTAL) as average,MAX(I.ID) as ID_IN,C.ID FROM INVOICE as I left join CUSTOMER as C on CUSTOMERID = C.ID WHERE TOTAL>200 GROUP BY C.ID having AVG(TOTAL)>200");


            while (resultSet.next()){
                System.out.println(resultSet.getObject(1).toString());
            }


            c.close();
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }

    }

    private static void importAdultDataset() throws SQLException, IOException {
        statement.execute("create table if not exists adult(age int, "
                + "workclass VARCHAR(50), " + "fnlwgt int, "
                + "education VARCHAR(50), " + "education.num int, "
                + "marital.status VARCHAR(50), " + "occupation VARCHAR(50), "
                + "relationship VARCHAR(50), " + "race VARCHAR(50), "
                + "sex VARCHAR(50), " + "capital.gain int, "
                + "capital.loss int, " + "hours.per.week int, "
                + "native.country VARCHAR(50), " + "income VARCHAR(50))");
        ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM adult");
        resultSet.next();
        if (resultSet.getInt(1)==0) {
            Stream st = Files.lines(Paths.get("./data/adult.csv"));
            st.forEach(Test::addAdult);
        }
    }

    private static void addAdult(Object o){
        String s = (String) o;
        s = s.replaceAll("\"","'");
        String[] values = s.split(",");
        try {
            statement.execute("insert into adult values("+
                    values[0]+","+values[1]+","+
                    values[2]+","+values[3]+","+
                    values[4]+","+values[5]+","+
                    values[6]+","+values[7]+","+
                    values[8]+","+values[9]+","+
                    values[10]+","+values[11]+","+
                    values[12]+","+values[13]+","+
                    values[14]+")");
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}