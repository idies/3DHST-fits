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
import uk.ac.starlink.util.FileDataSource;
import uk.ac.starlink.util.TestCase;
import uk.ac.starlink.util.URLDataSource;


public class FitsReader {

    public FitsReader(){};



    public void testRead(String filename) throws IOException {
        URL url = FitsReader.class.getResource( filename );
        StarTable table = new FitsTableBuilder()
                .makeStarTable( new URLDataSource( url ), true,
                        StoragePolicy.PREFER_MEMORY );
        table = StoragePolicy.PREFER_MEMORY.randomTable( table );

        System.out.println(table.getName());
        System.out.println(table.getRowCount());
        System.out.println(table.getColumnCount());
    }


    public static void main(String argv[]){

        FitsReader fr = new FitsReader();


        try {
            fr.testRead("0-9.fits");
            //fr.testRead("cosmos-01-F140W_asn.fits");
            fr.testRead("cosmos_3dhst.v4.1.5.zbest.fits");

        }
        catch(IOException e){
            e.printStackTrace();
        }





    }
}
