package main.java;

import utils.FileUtilities;

import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by shanmukh on 4/11/15.
 */
public class ASMParser implements Serializable {
    FileUtilities f;
    public ASMParser(){
        f = new FileUtilities();
    }

    public static void ParseASMFile(String filename, String fileContents) throws Exception {
        //Open the file and parse the asm file to return a string.
        StringTokenizer lines = new StringTokenizer(fileContents, System.getProperty("line.separator"));
        PrintWriter pw = new PrintWriter("/home/ssista/processedtest/" + filename + ".pasm");
        while (lines.hasMoreTokens()) {
            //Apply regular expression on each line
            String line = lines.nextToken();
            String outLine = ASMLineTransformerUtil.getTransformedCleanLine(line);
            if (!outLine.isEmpty() && !outLine.equals(System.getProperty("line.separator"))) {

                pw.println(outLine);

            }
        }
        pw.close();
    }

    /*
    Returns a hashmap of all the subroutines and their contents in a string.
     */
    public HashMap<String, String> ParseSubRoutinesInFile(String filename, String fileContents) throws Exception {
        Runtime runtime = Runtime.getRuntime();
        int mb = 1024*1024;
        //Print used memory
        System.out.println("Used Memory:"
                + (runtime.totalMemory() - runtime.freeMemory()) / mb);

        //Print free memory
        System.out.println("Free Memory:"
                + runtime.freeMemory() / mb);

        //Print total available memory
        System.out.println("Total Memory:" + runtime.totalMemory() / mb);

        //A subroutine has a start line sub_<Number>
        HashMap<String, String> routines = new HashMap<String, String>();
        StringTokenizer lines = new StringTokenizer(fileContents, System.getProperty("line.separator"));
        System.out.println("number of lines to process : " + lines.countTokens());
        PrintWriter pw = f.CreateOrOpenFile("/home/ssista/processedtest/temp/" + filename + ".processed");
        int linec = 0;

        while (lines.hasMoreTokens()) {
            //Apply regular expression on each line
            String outLine = lines.nextToken();
            //Get all subs from outLine
            if (outLine.length() >= 4) {
                String substring = outLine.substring(0, 4).trim();
                //System.out.println(substring);
                //Split using tab.
                /////The lines below will be executed if we find a sub call.
                if (substring.contains("sub_") && !outLine.contains("endp")) {
                    //recursively get all lines until sub_<<name>>\tend is encountered
                    String procName = outLine.split("\t")[0];
                    //pw.println(procName);
                    if ( lines.hasMoreTokens()){
                        String nextLine = lines.nextToken().trim();
                        //System.out.println("Subroutine Start");
                        String sub = "" ;
                        while (!nextLine.contains("endp")) {
                            if (lines.hasMoreTokens()){
                                sub += nextLine;
                                nextLine = lines.nextToken();
                                if ( !nextLine.contains("endp")){
                                    sub += System.getProperty("line.separator");
                                }
                            }
                            else{
                                break;
                            }
                        }
                        System.out.println("Exited Loop. Adding entry to hashmap.");
                        routines.put(procName,sub);
                    }
                    else{
                        break;
                    }
                }
                else{
                    pw.println(outLine);
                    //Write the line to file.
                }
            }
            else{
                pw.println(outLine);
                //write the contents to a new file.
            }
        }
        pw.close();
        //System.out.println(routines.toString());
        System.out.println("Generated map!");
        return routines;
    }
    public void ExpandRoutines(HashMap<String, String> rMap, String fileName, String fileContents) throws Exception{
        //Loop through the file contents and replace the file cont
        PrintWriter pw = f.CreateOrOpenFile("/home/ssista/processedtest/final/" + fileName + ".final");
        StringTokenizer lines = new StringTokenizer(fileContents, System.getProperty("line.separator"));
        List<String> linesString = Files.readAllLines(Paths.get("/home/ssista/processedtest/temp/" + fileName + ".processed"));
        for ( String line : linesString){
            if ( line.contains("call") || line.contains("jmp\tsub") || line.contains("jmp sub") ){
                // System.out.println("statemetn " + line);
                String[] pnames = line.split("\\t");
                String pname ="";
                if(pnames.length > 1){
                    pname = pnames[1];
                }
                if (rMap.containsKey(pname)){
                    //System.out.println("Procedure call"  + pname);
                    //System.out.println(rMap.get(pname));
                    pw.println(rMap.get(pname));

                }
                else{
                    pw.println();
                }
            }
            else{
                pw.println(line);
            }
        }
        pw.close();
        System.out.println("Generated final file for : " + fileName);
    }


}


final class ASMLineTransformerUtil {

    private static final String REGEX_NEWLINE_OR_SPACES = "(\n)|(\r\f?)|(\f)|(\r)|(\\s+)";

    // ((^header)|(^\\\\.text)|(^\\\\.rdata)|(^\\\\.data)|(^\\\\.rsrc)|(^\\\\.reloc)|(^\\\\.))
    private static final String REGEX_BEGIN_GNRL_HDR = "(?i)(^\\.?[a-z]+\\:[0-9A-F]+)\\s*";

    private static final String REGEX_COMMENT_LINE_END = ";.*$";

    private ASMLineTransformerUtil() {

    }

    public static String getTransformedCleanLine(String text) {
        // if it is newline or empty line don't do anything just return the text
        // itself
        if (isNonProcessableLine(text)) {
            return text;
        }
        // 1. first remove header, text, rdata, data from beginning of the line
        // reference and number followed by it
        text = replaceBeginLine(text);

        // 2. remove any comment line if any ie. anything after ;
        text = replaceCommentAfterPart(text);
        // 3. trim the text to remove all spaces in between and after
        text = text.trim();
        //Replace additional tabs
        text  = replaceExtraTabs(text);
        // 4.
        return text;
    }
    private static String replaceExtraTabs(String text){
        text=text.replaceAll("\\s+","\t");
        return text.replaceAll("^([A-F0-9][A-F0-9]\\s)+","");
    }
    private static String replaceCommentAfterPart(String text) {
        // TODO Auto-generated method stub
        return text.replaceAll(REGEX_COMMENT_LINE_END, "");
    }

    private static String replaceBeginLine(String text) {
        return text.trim().replaceAll(REGEX_BEGIN_GNRL_HDR, "");
    }

    private static boolean isNonProcessableLine(String text) {
        return text == null || text.trim().length() == 0
                || text.replaceAll(REGEX_NEWLINE_OR_SPACES, "").length() == 0;
    }
}
