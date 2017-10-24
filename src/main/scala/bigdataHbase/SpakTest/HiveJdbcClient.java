package bigdataHbase.SpakTest;

import java.sql.*;

public class HiveJdbcClient {


    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public boolean run() {

        try {
            Class.forName(driverName);
            Connection con = null;
            con = DriverManager.getConnection(
                    "jdbc:hive2://192.168.17.15:10000/hivedb", "hiveuser", "hiveuser");
            Statement stmt = con.createStatement();
            ResultSet res = null;

            String sql = "select count(*) from test_data";

            System.out.println("Running: " + sql);
            res = stmt.executeQuery(sql);
            System.out.println("ok");
            while (res.next()) {
                System.out.println(res.getString(1));

            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("error");
            return false;
        }

    }

    public static void main(String[] args) throws SQLException {
        HiveJdbcClient hiveJdbcClient = new HiveJdbcClient();
        hiveJdbcClient.run();
    }

}
