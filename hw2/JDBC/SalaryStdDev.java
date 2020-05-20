import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

public class SalaryStdDev {
    public static void main(String[] argv) {
        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver");
        }
        catch (ClassNotFoundException e) {
            System.out.println("Please include Classpath  Where your DB2 Driver is located");
            e.printStackTrace();
            return;
        }
//        "java SalaryStdDev databasename tablename login password"
//        for(int i=0;i< argv.length;i++)
//        {
//            System.out.println(argv[i]);
//        }
        String databasename = argv[0];
        String tablename = argv[1];
        String login = argv[2];
        String password =argv[3];
        System.out.println("DB2 driver is loaded successfully");
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rset=null;
        boolean found=false;
        List<Double> lst = new ArrayList<>();
        try {
            conn = DriverManager.getConnection("jdbc:db2://127.0.0.1:50000/" + databasename,login,password);
            if (conn != null)
            {
                System.out.println("DB2 Database Connected");
            }
            else
            {
                System.out.println("Db2 connection Failed ");
            }
            assert conn != null;
            pstmt=conn.prepareStatement("Select salary from " + tablename );
            rset=pstmt.executeQuery();
            if(rset!=null)
            {

                while(rset.next())
                {
                    found=true;
                    lst.add(Double.parseDouble(rset.getString(1)));
                }
            }
            if (!found)
            {
                System.out.println("No Information Found");
            }

            double sum = 0;
            double std = 0;
            int length = lst.size();
            double summation = 0;
            for(int i = 0; i< length; i++){
                sum = sum + lst.get(i) * lst.get(i);
                summation = summation + lst.get(i);
            }

            sum = sum / length;
            summation = summation /length;

            std = Math.sqrt(sum - summation * summation);
            System.out.println("The standard deviation of the salary is :" + std);

        } catch (SQLException e) {
            System.out.println("DB2 Database connection Failed");
            e.printStackTrace();
        }
    }

}