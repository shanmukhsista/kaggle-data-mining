package main.java; /**
 * Created by shanmukh on 3/17/15.
 */
import algorithms.BayesClassifier;
import org.apache.hadoop.util.hash.Hash;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.io.Serializable;
import java.sql.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.*;

import com.mysql.jdbc.Driver;

public class Main {
    public static ResultSet GetResultsForQuery(Connection conn,String query, boolean isUpdate){
        //connect to mysql database

        Statement stmt = null;
        ResultSet rs = null;
        try{
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            //System.out.println("Connecting to database");
            //conn = DriverManager.getConnection("jdbc:mysql://149.160.246.42/kaggledb", "root", "root");

            conn = DriverManager.getConnection("jdbc:mysql://localhost/kaggledb?rewriteBatchedStatements=true", "root", "root");
            stmt = conn.createStatement();
            if ( isUpdate){
                stmt.executeUpdate(query);
                conn.close();
            }
            else {
                rs = stmt.executeQuery(query);
            }
        }
        catch (SQLException se){
            System.out.println("Exception : " + se.toString());
        }
        catch (ClassNotFoundException ce ){
            System.out.println("Exception : " + ce.toString());
        }
        catch (IllegalAccessException ie ){
            System.out.println("Exception : " + ie.getMessage());
        }
        catch (InstantiationException iae){
            System.out.println("Exception : " + iae.getMessage());
        }
        return rs ;
    }
    public static void main( String[] args) throws Exception {
      final  HashMap<String, Short> labelsMap = new HashMap<String, Short>();
       /* String sql = "select mFileName, mLabelId from trainingsetlabels;";
        try{
            Connection conn = null;
            ResultSet rs = GetResultsForQuery(conn, sql,false);
            while ( rs.next()) {
                labelsMap.put(rs.getString("mFileName"), rs.getShort("mLabelId"));
            }
            //conn.close();
        }
        catch (SQLException se ){

        }
*/
        //First generate the training vector and then the test vector.
        //GenerateTrainingVector(labelsMap);
        //GenerateTestVector(labelsMap);
        //ParseASM();


        /*String fileName = "/home/ssista/processed*//**//*.pasm";
        SparkConf conf = new SparkConf().setAppName("ASM Parser");
        final JavaSparkContext context = new JavaSparkContext(conf);
        JavaPairRDD<String, String> filePairs = context.wholeTextFiles(fileName);
        filePairs.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                while (tuple2Iterator.hasNext()) {
                    Tuple2<String, String> row = tuple2Iterator.next();
                    System.out.println("Key is  " + row._1());
                    //row_2 has all the contents of the file
                    //for each line, tokenize
                    ASMParser p = new ASMParser();
                    HashMap<String, String> proceduresMap = p.ParseSubRoutinesInFile(GetFileNameOnly(row._1()), row._2());
                    if (proceduresMap != null) {
                        p.ExpandRoutines(proceduresMap, GetFileNameOnly(row._1()), row._2());
                    }
                    proceduresMap.clear();
                    p = null;
                }
            }
        });*/
        //Read();
        BayesClassifier c = new BayesClassifier("1");
        c.loadFileStringForClass();
        c.CollectFiles();
       // ParseASM();

          //ProcessPASMFiles();

    }

    public static void Read() throws Exception{
        String fileName = "/home/ssista/final/*.final";
        SparkConf conf = new SparkConf().setAppName("ASM Parser");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> files = context.textFile(fileName);


        JavaPairRDD<String, Integer> res = files.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split("(\\s)+"));
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<String, Integer>(s.trim(), 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }).setName(GetFileNameOnly(files.name()));
        List<Tuple2<String, Integer>> lines = res.collect();
        PrintWriter pw = new PrintWriter("/home/ssista/res.txt");
        for ( Tuple2<String, Integer> line : lines){
            pw.println(line._1() + "#" + line._2());
        }
        pw.close() ;
    }
    public static void MapPASMFiles(){
        String fileName = "/home/ssista/processed/*.pasm";
        SparkConf conf = new SparkConf().setAppName("ASM Parser");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaPairRDD<String, String> filePairs = context.wholeTextFiles(fileName);
        filePairs.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                while (tuple2Iterator.hasNext()) {
                    Tuple2<String, String> row = tuple2Iterator.next();
                    System.out.println("Key is  " + row._1());
                    //row_2 has all the contents of the file
                    //for each line, tokenize
                    ASMParser p = new ASMParser();
                    HashMap<String, String> proceduresMap = p.ParseSubRoutinesInFile(GetFileNameOnly(row._1()), row._2());
                    if (proceduresMap != null) {
                        p.ExpandRoutines(proceduresMap, GetFileNameOnly(row._1()), row._2());
                    }
                    proceduresMap.clear();
                }
            }
        });
    }

    public static void ProcessPASMFiles(){
        //String fileName = "/home/ssista/processed/*.pasm";
        String fileName = "/home/ssista/processedtest/*.pasm";
        SparkConf conf = new SparkConf().setAppName("ASM Parser");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaPairRDD<String, String> filePairs = context.wholeTextFiles(fileName);
        filePairs.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                while (tuple2Iterator.hasNext()) {
                    Tuple2<String, String> row = tuple2Iterator.next();
                    System.out.println("Key is  " + row._1());
                    //row_2 has all the contents of the file
                    //for each line, tokenize
                    ASMParser p = new ASMParser();
                    HashMap<String, String> proceduresMap = p.ParseSubRoutinesInFile(GetFileNameOnly(row._1()), row._2());
                    if ( proceduresMap != null){

                        p.ExpandRoutines(proceduresMap,GetFileNameOnly(row._1()), row._2());
                    }
                    proceduresMap.clear();
                }
            }
        });
    }
    public static void ParseASM(){
        String fileName = "/home/ssista/test/*.asm";
        SparkConf conf = new SparkConf().setAppName("ASM Parser");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaPairRDD<String, String> filePairs = context.wholeTextFiles(fileName);
        filePairs.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                while (tuple2Iterator.hasNext()) {
                    Tuple2<String, String> row = tuple2Iterator.next();
                    System.out.println("Key is  " + row._1());
                    //row_2 has all the contents of the file
                    //for each line, tokenize
                    StringBuilder s = new StringBuilder();
                    ASMParser.ParseASMFile(GetFileNameOnly(row._1()), row._2());
                }
            }
        });
        //System.out.println("First Partition" + f
    }
    public static void GenerateTestVector(final HashMap<String, Short> labelsMap){
        String fileName = "/media/f3012054-0912-4fd4-8447-d0bad1858df1/test/test/*.bytes";
        SparkConf conf = new SparkConf().setAppName("Test Vector Build Application");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaPairRDD<String, String> filePairs = context.wholeTextFiles(fileName);
        filePairs.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                while (tuple2Iterator.hasNext()) {
                    final HashMap<String, Short> labelsFinal = labelsMap;
                    Tuple2<String, String> row = tuple2Iterator.next();
                    System.out.println("Key is  " + row._1());
                    //row_2 has all the contents of the file
                    //for each line, tokenize
                    HashMap<Integer, Long> byteCount = new HashMap<Integer, Long>();
                    //long line = 0;
                    StringBuilder s = new StringBuilder();
                    StringTokenizer lineTokenizer = new StringTokenizer(row._2(), "\n");
                    //s.append(labelsFinal.get(GetFileNameOnly(row._1)) + " ");
                    while (lineTokenizer.hasMoreTokens()) {
                        String newLine = lineTokenizer.nextToken();
                        //line++;
                        //System.out.println("Line number : " + line);
                        StringTokenizer cols = new StringTokenizer(newLine, " ");
                        //Skip the first token

                        String offset = cols.nextToken();
                        while (cols.hasMoreTokens()) {
                            String hexVal = cols.nextToken().trim();
                            if (!hexVal.contains("?")) {
                                int byteVal = Integer.parseInt(hexVal, 16);
                                //check if value exists in map
                                if (byteCount.containsKey(byteVal)) {
                                    long c = byteCount.get(byteVal);
                                    byteCount.put(byteVal, c + 1);
                                } else {
                                    byteCount.put(byteVal, (long) 1);
                                }
                                //System.out.println("Hex val : " + hexVal.trim() + " Byte val : " + Integer.parseInt(hexVal.trim(), 16));
                            }
                            else{
                                /*
                                We have observed that * is a feature taht is repeated in every document.
                                 */
                                int byteVal = 256; //256 denotes a question mark.
                                //check if value exists in map
                                if (byteCount.containsKey(byteVal)) {
                                    long c = byteCount.get(byteVal);
                                    byteCount.put(byteVal, c + 1);
                                } else {
                                    byteCount.put(byteVal, (long) 1);
                                }
                            }
                        }

                    }
                    for (int k : byteCount.keySet()) {
                        s.append(k+1);
                        s.append(":");
                        s.append(byteCount.get(k));
                        s.append(" ");
                    }
                    //System.out.println("Final String to write " + s.toString());
                    //Insert the string into a database
                    Connection conn = null;
                    String sql = "INSERT INTO `testdatavector`(`filename`, `dataString`) VALUES (  '" + GetFileNameOnly(row._1())  +  "', ' "  + s.toString()  + "' );";
                    ResultSet rs = GetResultsForQuery(conn,sql,true);
                    //System.out.println("Inserted Data");
                    //conn.close();
                    //ResultSet rs = stmt.executeQuery(sql);
                    //FileWriter writer = new FileWriter("files/out/_" + GetFileNameOnly(row._1) + ".part");
                    //BufferedWriter out = new BufferedWriter(writer);
                    //out.write(s.toString());
                    //out.close();
                    //writer.close();
                }
            }
        });
        //System.out.println("First Partition" + f
    }

    public static void GenerateTrainingVectorPair(final  HashMap<String, Short> labelsMap){
        String fileName = "/media/f3012054-0912-4fd4-8447-d0bad1858df1/train/*.bytes";
        SparkConf conf = new SparkConf().setAppName("Sample Application");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaPairRDD<String, String> filePairs = context.wholeTextFiles(fileName);
        final HashSet<Integer> excludedBytes = new HashSet<Integer>();
        excludedBytes.add(0);
        filePairs.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                while (tuple2Iterator.hasNext()) {
                    final HashMap<String, Short> labelsFinal = labelsMap;
                    Tuple2<String, String> row = tuple2Iterator.next();
                    System.out.println("Key is  " + row._1());
                    //row_2 has all the contents of the file
                    //for each line, tokenize
                    HashMap<Integer, Long> byteCount = new HashMap<Integer, Long>();
                    //long line = 0;
                    StringBuilder s = new StringBuilder();
                    StringTokenizer lineTokenizer = new StringTokenizer(row._2(), "\n");
                    StringBuilder batchSql = new StringBuilder();
                    s.append(labelsFinal.get(GetFileNameOnly(row._1())) + " ");
                    while (lineTokenizer.hasMoreTokens()) {
                        String newLine = lineTokenizer.nextToken();
                        //line++;
                        //System.out.println("Line number : " + line);
                        StringTokenizer cols = new StringTokenizer(newLine, " ");
                        //Skip the first token

                        String offset = cols.nextToken();
                        while (cols.hasMoreTokens()) {
                            String hexVal = cols.nextToken().trim();
                            String hexVal2 = cols.nextToken().trim();
                            if (!hexVal.contains("?")) {
                                int byteVal = Integer.parseInt(hexVal, 16);
                                int byteVal2 = Integer.parseInt(hexVal2, 16);
                                if (!excludedBytes.contains(byteVal)){
                                    //check if value exists in map
                                    if (byteCount.containsKey(byteVal)) {
                                        long c = byteCount.get(byteVal + byteVal2);
                                        byteCount.put(byteVal + byteVal2, c + 1);
                                    } else {
                                        byteCount.put(byteVal + byteVal2, (long) 1);
                                    }
                                }
                                //System.out.println("Hex val : " + hexVal.trim() + " Byte val : " + Integer.parseInt(hexVal.trim(), 16));
                            } else {
                                /*
                                We have observed that * is a feature taht is repeated in every document.
                                 */
                                int byteVal = 256; //256 denotes a question mark.
                                //check if value exists in map
                                if (byteCount.containsKey(byteVal)) {
                                    long c = byteCount.get(byteVal);
                                    byteCount.put(byteVal, c + 1);
                                } else {
                                    byteCount.put(byteVal, (long) 1);
                                }
                            }
                        }

                    }
                    //batchSql.append("INSERT INTO `kaggledb`.`bytecount` (`filename`,`bytevalue`,`bytecount`) VALUES ");
                    for (int k : byteCount.keySet()) {
                        //batchSql.append("  ('" + GetFileNameOnly(row._1) + "'," + (k + 1) + " , " + byteCount.get(k) + "),");

                        s.append(k + 1);
                        s.append(":");
                        s.append(byteCount.get(k));
                        s.append(" ");
                    }
                    //System.out.println("Final String to write " + s.toString());
                    //Insert the string into a database
                    Connection conn = null;
                    String sql = "INSERT INTO `kaggledb`.`trainingDataVector`(`mLabelId`, `dataString`) VALUES (  '" + labelsFinal.get(GetFileNameOnly(row._1)) + "', ' " + s.toString() + "' );";
                    ResultSet rs = GetResultsForQuery(conn, sql, true);
                    //  GetResultsForQuery(conn, batchSql.substring(0, (batchSql.length() - 1)), true);
                    //System.out.println("Inserted Data");
                    //conn.close();
                    //ResultSet rs = stmt.executeQuery(sql);
                    //FileWriter writer = new FileWriter("files/out/_" + GetFileNameOnly(row._1) + ".part");
                    //BufferedWriter out = new BufferedWriter(writer);
                    //out.write(s.toString());
                    //out.close();
                    //writer.close();
                }
            }
        });
        //System.out.println("First Partition" + f
    }

    public static void ReadFile( ) throws Exception {
        final String fileName = "/home/shanmukh/Desktop/data/*.txt";
        SparkConf conf = new SparkConf().setAppName("ASM Parser");
        JavaSparkContext context = new JavaSparkContext(conf);

        /*filePairs.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,String>>, Object>() {
            @Override
            public Iterable<Object> call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                while ( tuple2Iterator.hasNext()){
                    Object row = tuple2Iterator.next();
                }
                return null;
            }
        });*/
        JavaPairRDD<String, String> filePairs = context.wholeTextFiles(fileName,5);
        List<Tuple2<String, String>> lines = filePairs.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> stringStringTuple2) throws Exception {

                return new Tuple2<String, String>(stringStringTuple2._1(), stringStringTuple2._2());
            }
        }).collect();
        PrintWriter pw = new PrintWriter("/home/shanmukh/Desktop/spark.txt");
        for ( Tuple2<String, String> line : lines ){
            pw.println(line._2() + "t");

        }
        pw.close();
    }
    public static void GenerateTrainingVector(final  HashMap<String, Short> labelsMap){
        String fileName = "/media/f3012054-0912-4fd4-8447-d0bad1858df1/train/*.bytes";
        SparkConf conf = new SparkConf().setAppName("Sample Application");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaPairRDD<String, String> filePairs = context.wholeTextFiles(fileName);
        final HashSet<Integer> excludedBytes = new HashSet<Integer>();
        excludedBytes.add(0);
        filePairs.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
            @Override
            public void call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                while (tuple2Iterator.hasNext()) {
                    final HashMap<String, Short> labelsFinal = labelsMap;
                    Tuple2<String, String> row = tuple2Iterator.next();
                    System.out.println("Key is  " + row._1());
                    //row_2 has all the contents of the file
                    //for each line, tokenize
                    HashMap<Integer, Long> byteCount = new HashMap<Integer, Long>();
                    //long line = 0;
                    StringBuilder s = new StringBuilder();
                    StringTokenizer lineTokenizer = new StringTokenizer(row._2(), "\n");
                    StringBuilder batchSql = new StringBuilder();
                    s.append(labelsFinal.get(GetFileNameOnly(row._1())) + " ");
                    while (lineTokenizer.hasMoreTokens()) {
                        String newLine = lineTokenizer.nextToken();
                        //line++;
                        //System.out.println("Line number : " + line);
                        StringTokenizer cols = new StringTokenizer(newLine, " ");
                        //Skip the first token

                        String offset = cols.nextToken();
                        while (cols.hasMoreTokens()) {
                            String hexVal = cols.nextToken().trim();
                            if (!hexVal.contains("?")) {
                                int byteVal = Integer.parseInt(hexVal, 16);
                                if (!excludedBytes.contains(byteVal)){
                                    //check if value exists in map
                                    if (byteCount.containsKey(byteVal)) {
                                        long c = byteCount.get(byteVal);
                                        byteCount.put(byteVal, c + 1);
                                    } else {
                                        byteCount.put(byteVal, (long) 1);
                                    }
                                }

                                //System.out.println("Hex val : " + hexVal.trim() + " Byte val : " + Integer.parseInt(hexVal.trim(), 16));
                            } else {
                                /*
                                We have observed that * is a feature taht is repeated in every document.
                                 */
                                int byteVal = 256; //256 denotes a question mark.
                                //check if value exists in map
                                if (byteCount.containsKey(byteVal)) {
                                    long c = byteCount.get(byteVal);
                                    byteCount.put(byteVal, c + 1);
                                } else {
                                    byteCount.put(byteVal, (long) 1);
                                }
                            }
                        }

                    }
                    //batchSql.append("INSERT INTO `kaggledb`.`bytecount` (`filename`,`bytevalue`,`bytecount`) VALUES ");
                    for (int k : byteCount.keySet()) {
                        //batchSql.append("  ('" + GetFileNameOnly(row._1) + "'," + (k + 1) + " , " + byteCount.get(k) + "),");

                        s.append(k + 1);
                        s.append(":");
                        s.append(byteCount.get(k));
                        s.append(" ");
                    }
                    //System.out.println("Final String to write " + s.toString());
                    //Insert the string into a database
                    Connection conn = null;
                    String sql = "INSERT INTO `kaggledb`.`trainingDataVector`(`mLabelId`, `dataString`) VALUES (  '" + labelsFinal.get(GetFileNameOnly(row._1)) + "', ' " + s.toString() + "' );";
                    ResultSet rs = GetResultsForQuery(conn, sql, true);
                    //  GetResultsForQuery(conn, batchSql.substring(0, (batchSql.length() - 1)), true);
                    //System.out.println("Inserted Data");
                    //conn.close();
                    //ResultSet rs = stmt.executeQuery(sql);
                    //FileWriter writer = new FileWriter("files/out/_" + GetFileNameOnly(row._1) + ".part");
                    //BufferedWriter out = new BufferedWriter(writer);
                    //out.write(s.toString());
                    //out.close();
                    //writer.close();
                }
            }
        });
        //System.out.println("First Partition" + f
    }

    public static String GetFileNameOnly(String path){
        String nameOnly = "";
        //Split using /
        String firstPart = path.split("\\.")[0];
        if ( firstPart.contains("/")){
            String[] sArray = firstPart.split("/");
            nameOnly = sArray[sArray.length - 1];
        }
        else{
            nameOnly = firstPart;
        }
            return nameOnly;
    }


}


