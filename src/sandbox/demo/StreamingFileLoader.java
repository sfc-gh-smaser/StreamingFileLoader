package snowflake.demo;
import java.io.FileInputStream;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.Base64;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.simple.SimpleLogger;
import java.lang.reflect.Field;
import net.snowflake.ingest.utils.Logging;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

/**
 * Example on how to use the Streaming Ingest client APIs
 * Re-used File Loading Demonstration (Feb, 2023)
 *   Avoid added latency, costs, and security risks of loading your data in your Cloud Provider's object/file stores
 *            Author:  Steven.Maser@snowflake.com
 *
 * <p>Please read the README.md file for detailed steps and expanding beyond this example
 *   https://github.com/snowflakedb/snowflake-ingest-java
 */
public class StreamingFileLoader {
  private static boolean DEBUG=false;
  private Logger LOGGER = new Logging(this.getClass()).getLogger();
  private static ArrayList<String> COLUMNS=new ArrayList<String>();
  private static String FILE=null;
  private static String DELIM=",";

  public static void main(String[] args) throws Exception {
    if(args==null || args.length<2) throw new Exception ("Input Argument Required for File Path");
    File f=new File(args[0]);
    if(!f.exists()) throw new Exception ("Property file:  "+args[0]+" Does not exist!");
    FILE=args[1];
    //Load profile from properties file
    Properties props=loadProfile(f);

    // Create a streaming ingest client
    try (SnowflakeStreamingIngestClient client =
        SnowflakeStreamingIngestClientFactory.builder("CLIENT").setProperties(props).build()) {

      // Create an open channel request on table T_STREAMINGINGEST
      OpenChannelRequest request1 =
          OpenChannelRequest.builder(props.getProperty("channel_name"))
              .setDBName(props.getProperty("database"))
              .setSchemaName(props.getProperty("schema"))
              .setTableName(props.getProperty("table"))
              .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
              .build();

      // Open a streaming ingest channel from the given client
      SnowflakeStreamingIngestChannel channel1 = client.openChannel(request1);

      // corresponds to the row number
      long startTime = System.nanoTime();

      // get file
      BufferedReader reader = new BufferedReader(new FileReader(FILE));
        // read line from file and send to Snowflake as a row

      int id=0;
      int actual_line=0;
      int bad=0;
      String line;
      while((line=reader.readLine())!=null) {
        actual_line++;
        // A Map for all columns in a row
        Map<String, Object> row = new HashMap<>();

        // split file line into fields
        ArrayList<Object> output = new ArrayList<Object>();
        for (String s : line.split(",")) output.add(s);
        // load file line as a row
        if(DEBUG) System.out.println("Loading Line #"+actual_line);
        if(output.size()!=COLUMNS.size()) {
          System.out.println("Skipping line " + id + 1 + " Column/Delimiter Mismatch");
          bad++;
        }
        else {
          for (int i = 0; i < COLUMNS.size(); i++) {
            row.put(COLUMNS.get(i), output.get(i));
          }

          InsertValidationResponse response = channel1.insertRow(row, String.valueOf(id));
          if (response.hasErrors()) {
            // Simply throw exception at first error
            throw response.getInsertErrors().get(0).getException();
          }
          id++;
        }
      }
      System.out.println("Rows Sent:  "+id+1);
      System.out.println("Rows Skipped:  "+bad);
      System.out.println("Time to Send:  "+String.format("%.03f", (System.nanoTime() - startTime)*1.0/1000000000)+ " seconds");
      // Polling Snowflake to confirm delivery (using fetch offset token registered in Snowflake)
      int retryCount = 0;
      int maxRetries = 100;
      String expectedOffsetTokenInSnowflake=String.valueOf(id-1);
      //String offsetTokenFromSnowflake = channel1.getLatestCommittedOffsetToken();
      for (String offsetTokenFromSnowflake = channel1.getLatestCommittedOffsetToken();offsetTokenFromSnowflake == null
          || !offsetTokenFromSnowflake.equals(expectedOffsetTokenInSnowflake);) {
        if(DEBUG) System.out.println("Offset:  "+offsetTokenFromSnowflake);
        Thread.sleep(id/1000*COLUMNS.size());
        offsetTokenFromSnowflake = channel1.getLatestCommittedOffsetToken();
        retryCount++;
        if (retryCount >= maxRetries) {
          System.out.println(
              String.format(
                  "Failed to look for required OffsetToken in Snowflake:%s after MaxRetryCounts:%s (%S)",
                  expectedOffsetTokenInSnowflake, maxRetries, offsetTokenFromSnowflake));
          System.exit(1);
        }
      }
      System.out.println("SUCCESSFULLY inserted " + id+1 + " rows");
      System.out.println("Total Time, including Confirmation:  "+String.format("%.03f", (System.nanoTime() - startTime)*1.0/1000000000)+ " seconds");
    }
  }

  private static Properties loadProfile(File f) throws Exception {
    Properties props = new Properties();
    try {
      FileInputStream resource = new FileInputStream(f);
      props.load(resource);
      DELIM=props.getProperty("delimiter",DELIM);
      String columns=props.getProperty("columns",null);
      if(columns==null) throw new Exception ("Property 'columns' is required!");
      for (String s : columns.split(",")) COLUMNS.add(String.valueOf(s));
      String debug = props.getProperty("DEBUG");
      if (debug != null) DEBUG = Boolean.parseBoolean(debug);
      if (DEBUG) {
        for (Object key : props.keySet())
          System.out.println("  * DEBUG: " + key + ": " + props.getProperty(key.toString()));
      }
      else System.setProperty(org.slf4j.simple.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "ERROR");
      if (props.getProperty("private_key_file") != null) {
        String keyfile = props.getProperty("private_key_file");
        File key = new File(keyfile);
        if (!(key).exists()) throw new Exception("Unable to find key file:  " + keyfile);
        String pkey = readPrivateKey(key);
        props.setProperty("private_key", pkey);
      }
      props.setProperty("scheme","https");
      props.setProperty("port","443");
    } catch (Exception ex) {
        ex.printStackTrace();
        System.exit(-1);
    }
    return props;
  }

  private static String readPrivateKey(File file) throws Exception {
    String key = new String(Files.readAllBytes(file.toPath()), Charset.defaultCharset());
    String privateKeyPEM = key
            .replace("-----BEGIN PRIVATE KEY-----", "")
            .replaceAll(System.lineSeparator(), "")
            .replace("-----END PRIVATE KEY-----", "");
    if(DEBUG) {  // check key file is valid
      byte[] encoded = Base64.getDecoder().decode(privateKeyPEM);
      KeyFactory keyFactory = KeyFactory.getInstance("RSA");
      PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);
      RSAPrivateKey k = (RSAPrivateKey) keyFactory.generatePrivate(keySpec);
      System.out.println("* DEBUG: Provided Private Key is Valid:  ");
    }
    return privateKeyPEM;
  }
}

