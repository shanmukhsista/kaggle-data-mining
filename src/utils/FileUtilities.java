package utils;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.zip.*;

/**
 * Created by shanmukh on 4/12/15.
 */
public class FileUtilities {
    FileOutputStream fos ;
    ZipOutputStream zos ;
    public  PrintWriter CreateOrOpenFile(String filePath) throws Exception{
        PrintWriter pw = new PrintWriter(filePath);
        return pw ;
    }
}
