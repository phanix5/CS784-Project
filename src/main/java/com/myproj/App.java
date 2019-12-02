package com.myproj;
import java.sql.*;

/**
 * Hello world!
 */
public final class App {
    private App() {
    }

    /**
     * Says hello to the world.
     * @param args The arguments of the program.
     */
    public static void main(String[] args) {
        Connection c = null;
      try {
         Class.forName("org.postgresql.Driver");
         if(args.length!=8){
            System.out.println("Arguments expected: DBname Table1name Col1 Col2 Table2name Col1 Col2 Tau");
            System.exit(0);
         }
         String dbName = args[0];

         QueryContext ctx = new QueryContext(args[1],args[2],args[3],args[4],args[5],args[6], Double.parseDouble(args[7]));

         c = DriverManager
            .getConnection("jdbc:postgresql://localhost:5432/"+dbName+"?currentSchema=public&user=postgres&password=postgres");
         c.setAutoCommit(false);
         Query twoPath = new TwoPath(c, ctx);
         twoPath.preProcess();
         twoPath.evaluate(new String[]{"2","2"});
      } catch (Exception e) {
         e.printStackTrace();
         System.err.println(e.getClass().getName()+": "+e.getMessage());
         System.exit(0);
      }
      
    }
}
