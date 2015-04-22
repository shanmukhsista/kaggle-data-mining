package algorithms;
        import akka.remote.transport.ThrottledAssociation;
        import org.apache.hadoop.util.hash.Hash;
        import org.apache.spark.Accumulator;
        import org.apache.spark.SparkContext;
        import org.apache.spark.api.java.*;
        import org.apache.spark.SparkConf;
        import org.apache.spark.api.java.function.*;
        import scala.Tuple2;
        import scala.tools.nsc.transform.patmat.MatchAnalysis;

        import java.io.*;
        import java.math.BigDecimal;
        import java.nio.file.Files;
        import java.nio.file.Paths;
        import java.sql.*;

        import java.util.*;
        import java.util.concurrent.ExecutorService;
        import java.util.concurrent.Executors;

/**
 * Created by shanmukh on 4/16/15.
 */
public class BayesClassifier  implements Serializable {
    double prior ;
    transient SparkConf conf = new SparkConf().setAppName("ASM Parser").set("spark.driver.allowMultipleContexts", "true");
    transient  JavaSparkContext context = new JavaSparkContext(conf) ;
    String classLabel ;
    final Map<String, Double> labelsMap;
    public BayesClassifier(String classLabel){
        this.classLabel = classLabel ;
        labelsMap = ComputeClassProbabilities();
    }

    public void CollectFiles() throws Exception{
        //check if the file exists
        File f =  new File("/home/ssista/objs/" + classLabel + ".sobj");
        if ( f.exists()){
            //Read from the file and cast it into a maa.
           Map<String, Double> probMap = (Map<String, Double>)ReadObject("/home/ssista/objs/" + classLabel + ".sobj");
           System.out.println("Load complete.");
           // System.out.println(probMap);
           TestFiles(probMap);
        }
        else{
            f.createNewFile();
            //Read file from text file.
            String labelsPath = loadFileStringForClass();
            labelsPath = labelsPath.substring(0,labelsPath.length() - 1);
            JavaRDD<String> files = context.textFile(labelsPath);
            final Accumulator<Double> totalWords = context.accumulator(0.0);
            JavaPairRDD<String, Long> training  = files.flatMap(new FlatMapFunction<String, String>() {
                @Override
                public Iterable<String> call(String s) throws Exception {
                    return Arrays.asList(s.split("(\\s)+"));
                }
            }).mapToPair(new PairFunction<String, String, Long>() {
                @Override
                public Tuple2<String, Long> call(String s) throws Exception {
                    totalWords.add(1.0);
                    return new Tuple2<String, Long>(s, (long) 1);
                }
            }).reduceByKey(new Function2<Long, Long, Long>() {
                @Override
                public Long call(Long aLong, Long aLong2) throws Exception {
                    return aLong + aLong2;
                }
            });
            final Long count = training.count();
            final long tw = totalWords.value().longValue();
            Map<String, Double> probMap= training.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Long>, String, Double>() {
                @Override
                public Iterable<Tuple2<String, Double>> call(Tuple2<String, Long> stringLongTuple2) throws Exception {
                    double prob = ((stringLongTuple2._2()  + 1 ))/((tw + count)*1.0);
                    return Arrays.asList(new Tuple2<String, Double>(stringLongTuple2._1(), prob ));
                }
            }).collectAsMap();
            //Test a file
            SaveObject(probMap,"/home/ssista/objs/" + classLabel + ".sobj");
            TestFiles(probMap);
        }
       // TestFiles(probMap);
    }
    public void TestFiles(final Map<String, Double> pMap) throws Exception{
        Double presult =Math.log(this.prior);
        //PrintWriter pw = new PrintWriter("/home/ssista/"+classLabel + ".result");
        JavaPairRDD<String, String> files = context.wholeTextFiles("/home/ssista/finaltest/*.final");
        files.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
                double presult = prior;
                System.out.println("Processing file " + GetFileNameOnly(stringStringTuple2._1()));
                PrintWriter outFile = new PrintWriter(new BufferedWriter(new FileWriter("/home/ssista/" + classLabel + ".result", true)));
                StringTokenizer  tk = new StringTokenizer(stringStringTuple2._2(),"\n\t \r");
                    while (tk.hasMoreTokens()){
                        //if word is present in the dict,update the
                        //probability sum
                        String word = tk.nextToken();
                        if ( pMap.containsKey(word)){
                            prior = prior + Math.log(pMap.get(word));
                        }
                    }
                //Document parsing ccomplete
                //NOw save the value to a file.
               outFile.println(GetFileNameOnly(stringStringTuple2._1()) + ";" + prior);
                outFile.close();
            }
        });
        /*Files.walk(Paths.get("/home/ssista/finaltest/")).forEach(filePath -> {
            if (Files.isRegularFile(filePath)) {
                JavaRDD<String> files = context.textFile(filePath.toString());
                Double testResult = files.flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public Iterable<String> call(String s) throws Exception {
                        return Arrays.asList(s.split("(\\s)+"));
                    }
                }).mapToPair(new PairFunction<String, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(String s) throws Exception {
                        return new Tuple2<String, Long>(s, 1L);
                    }
                }).reduceByKey(new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long aLong, Long aLong2) throws Exception {
                        return aLong + aLong2;
                    }
                }).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Long>, String, Double>() {
                    @Override
                    public Iterable<Tuple2<String, Double>> call(Tuple2<String, Long> stringLongTuple2) throws Exception {
                        Double p = 0.0;
                        double prob;
                        if (pMap.containsKey(stringLongTuple2._1())) {
                            p = pMap.get(stringLongTuple2._1());
                            prob=Math.log(p) * stringLongTuple2._2();
                        } else {
                            prob = p;
                        }
                        return Arrays.asList(new Tuple2<String, Double>(stringLongTuple2._1(), prob));
                    }
                }).values().reduce(new Function2<Double, Double, Double>() {
                    @Override
                    public Double call(Double aDouble, Double aDouble2) throws Exception {
                        return aDouble + aDouble2;
                    }
                });
                Double res = testResult + presult;
                pw.println(GetFileNameOnly(filePath.toString()) + ";" + res);
            }*/


    }
    public String loadFileStringForClass(){
        String homeFolder = "/home/ssista/final/";
        StringBuffer b = new StringBuffer();
        int count = 0 ;
        for ( String key : this.labelsMap.keySet()){
            if ((labelsMap.get(key) == Double.parseDouble(this.classLabel))){
                b.append(homeFolder+ key + ".final,");
                count++;
            }
        }

        this.prior = count / ((1.0 ) * labelsMap.size());
        return b.toString();
    }

    public void LoadClassifier(){

    }



    public Map<String, Double> ComputeClassProbabilities(){

        Map<String, Double> result = new HashMap<String, Double>();
        //Read file from text file.
        String labelsPath = "/home/ssista/labels.lbl";
        JavaRDD<String> files = context.textFile(labelsPath);
         result = files.mapToPair(new PairFunction<String, String, Double>() {
            @Override
            public Tuple2<String, Double> call(String s) throws Exception {
                String[] temp = s.split("#");
                return new Tuple2(temp[0], Double.parseDouble(temp[1]));
            }
        }).collectAsMap();
        return result ;
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
    private void SaveObject(Object objectToSave, String path) throws Exception
    {
        FileOutputStream fos = new FileOutputStream(path);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(objectToSave);
        oos.close();
        fos.close();
        System.out.println("Finished writing the map file.");
    }
    private Object ReadObject( String path) throws Exception{
        FileInputStream fis = new FileInputStream(path);
        ObjectInputStream ois = new ObjectInputStream(fis);
        Object ret = ois.readObject();
        ois.close();
        System.out.println("Finished Retreiving Oject");
        return ret;

    }
}
