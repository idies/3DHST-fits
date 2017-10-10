/**
 * Created by swerner on 1/9/2017.
 */


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
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
import uk.ac.starlink.util.DataSource;
import uk.ac.starlink.util.FileDataSource;
import uk.ac.starlink.util.TestCase;
import uk.ac.starlink.util.URLDataSource;

import java.sql.*;



public class FitsReader {

    public FitsReader(){



    };



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
            System.out.println(jf.getCreateStatement(table.getName()));
            System.out.println(jf.getInsertStatement(table.getName()));
            jf.createJDBCTable(table.getName(), WriteMode.DROP_CREATE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public StarTable testRead(String filename) throws IOException {
        //URL url = FitsReader.class.getResource( filename );
        //DataSource source = new FileDataSource(new File(filename));
        StarTable table = new FitsTableBuilder()
                .makeStarTable( new FileDataSource( new File(filename) ), true,
                        StoragePolicy.PREFER_MEMORY );
        table = StoragePolicy.PREFER_MEMORY.randomTable( table );

        return table;

    }

    public List<String> getFitsFileList(Connection conn, int nRows) throws SQLException{

        List<String> fileList = new ArrayList<String>();
        //get list of fits files from table
        String cmd = null;
        /*
        SELECT TOP 1000
                [objectId]
      ,[catalogueId]
      ,[fileStorageId]
      ,[accessURL]
      ,[comment]
      ,[updatetimestamp]
        FROM [Cosmos3dHST].[fits].[FitsFile]
       */
        if (nRows > 0)
            cmd = String.format("SELECT top %d  objectID, accessURL from fits.FitsFile where objectID >= 1000000009", nRows);
                    else cmd=("SELECT objectID, accessURL FROM fits.FitsFile");



            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(cmd);

            while (rs.next()){
                String filename = rs.getString("accessURL");
                fileList.add(filename);

            }




    return fileList;

    }


    public static void main(String argv[]){

        FitsReader fr = new FitsReader();

        StarTable table = null;
        Connection conn = null;
        try {
            conn = fr.dbConnect(meta_url, driver, username, password);

            fileList = fr.getFitsFileList(conn, 10);
            conn.close();
        } catch( SQLException e) {
            e.printStackTrace();
        }



        try {

            //String filename = "cosmos-01-F140W_asn.fits";   DOES NOT WORK: uk.ac.starlink.table.TableFormatException: Got wrong row length: 107 != 55

            //String filename = "0-9.fits";
            //INFO: INSERT INTO file_H_GitHub_3DHST_fits_out_production_3DHST_fits_0_9_fits VALUES( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
            //java.sql.SQLException: Unable to convert between [S and LONGVARBINARY.

            //String filename = "cosmos_3dhst.v4.1.5.zbest.fits";

            //fr.testRead("cosmos-01-F140W_asn.fits");

            //table = fr.testRead("cosmos_3dhst.v4.1.5.zbest.fits");

            for (String filename : fileList) {


                table = fr.testRead(filename);


                System.out.println(table.getName());
                System.out.println(table.getRowCount());
                System.out.println(table.getColumnCount());


                conn = fr.dbConnect(url, driver, username, password);
                DatabaseMetaData dbm = conn.getMetaData();
                System.out.println(dbm.toString());

                fr.writeToDB(conn, table);
                System.out.println(String.format("Loaded %s", table.getName()));

            }
        } catch(IOException e) {
            e.printStackTrace();
        } catch(SQLException e) {
            e.printStackTrace();
        }








    }
}
