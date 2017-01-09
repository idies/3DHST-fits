/**
 * Created by swerner on 1/9/2017.
 */


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;
import nom.tam.fits.Header;
import nom.tam.fits.HeaderCardException;
import nom.tam.util.BufferedDataOutputStream;
import uk.ac.starlink.table.ArrayColumn;
import uk.ac.starlink.table.ColumnData;
import uk.ac.starlink.table.ColumnStarTable;
import uk.ac.starlink.table.RowSequence;
import uk.ac.starlink.table.StarTable;
import uk.ac.starlink.table.StarTableFactory;
import uk.ac.starlink.fits.FitsTableBuilder;
import uk.ac.starlink.table.StarTableOutput;
import uk.ac.starlink.table.StoragePolicy;
import uk.ac.starlink.table.Tables;
import uk.ac.starlink.table.formats.TextTableWriter;
import uk.ac.starlink.table.jdbc.JDBCFormatter;
import uk.ac.starlink.table.jdbc.WriteMode;
import uk.ac.starlink.util.FileDataSource;
import uk.ac.starlink.util.TestCase;
import uk.ac.starlink.util.URLDataSource;

import java.sql.*;



public class FitsReader {

    public FitsReader(){};



    public Connection dbConnect(String url, String driver, String username, String password) throws SQLException{
        Connection conn = null;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return conn;



    }

    public void writeToDB(Connection conn, StarTable table) {
        try {
            JDBCFormatter jf = new JDBCFormatter(conn, table);
            //System.out.println(jf.getCreateStatement(table.getName()));
            jf.createJDBCTable(table.getName(), WriteMode.CREATE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public StarTable testRead(String filename) throws IOException {
        URL url = FitsReader.class.getResource( filename );
        StarTable table = new FitsTableBuilder()
                .makeStarTable( new URLDataSource( url ), true,
                        StoragePolicy.PREFER_MEMORY );
        table = StoragePolicy.PREFER_MEMORY.randomTable( table );

        return table;

    }


    public static void main(String argv[]){

        FitsReader fr = new FitsReader();

        StarTable table = null;
        Connection conn = null;
        try {
            //fr.testRead("0-9.fits");
            //fr.testRead("cosmos-01-F140W_asn.fits");
            table = fr.testRead("cosmos_3dhst.v4.1.5.zbest.fits");

            System.out.println(table.getName());
            System.out.println(table.getRowCount());
            System.out.println(table.getColumnCount());



        }
        catch(IOException e){
            e.printStackTrace();
        }

        String url = "jdbc:jtds:sqlserver://sciserver01;DatabaseName=hst";
        String driver = "net.sourceforge.jtds.jdbc.Driver";
        String username = "hstuser";
        String password = "hst111";

        try {
            conn = fr.dbConnect(url, driver, username, password);
            DatabaseMetaData dbm = conn.getMetaData();
            System.out.println(dbm.toString());

            fr.writeToDB(conn, table);

        } catch (SQLException e) {
            e.printStackTrace();
        }









    }
}
