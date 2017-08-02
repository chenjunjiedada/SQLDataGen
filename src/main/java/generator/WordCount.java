package generator; /**
 * Created by root on 7/24/17.
 */

import jline.internal.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.util.*;

public class WordCount {
    /**
     * customized map function
     * get key-value from RandomRecordReader.nextkeyvalue()
     * key is the text
     * value does not used
     */
    public static class ShuffleMapper extends Mapper<Text,NullWritable,Text,NullWritable> {
        @Override
        public void map(Text key, NullWritable value, Context context) throws IOException, InterruptedException {
            context.write(key,value);
        }
    }

    /**
     * customized reduce function
     * key is the text
     * value does not used
     * sorted by key
     * write to the file
     */
    public static class ShuffleReducer extends Reducer<Text,NullWritable,Text,NullWritable> {
        @Override
        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
                context.write(key,NullWritable.get());
        }
    }
    /**
     * customized partition function,decide which reducer to join by the first 4 numbers in string 
     */
    public static class ShufflePartitioner extends Partitioner<Text,NullWritable> {
        @Override
        public int getPartition(Text text, NullWritable nullWritable, int numReduceTasks) {
            return Integer.valueOf(text.toString().substring(0,4))/(10000/numReduceTasks+1);
        }
    }

    public static final String NUM_MAP_TASKS = "random.generator.map.tasks";
    public static final String NUM_RECORDS_PER_TASK = "random.generator.num.records.per.map.task";

    /**
     * the customized inputformat which map side will use to get data
     */
    public static class RandomInputFormat extends InputFormat<Text, NullWritable> {

        @Override
        public List<InputSplit> getSplits(JobContext job) throws IOException {

            // Get the number of map tasks configured for
            int numSplits = job.getConfiguration().getInt(NUM_MAP_TASKS, -1);
            if (numSplits <= 0) {
                throw new IOException(NUM_MAP_TASKS + " is not set.");
            }

            // Create a number of input splits equivalent to the number of tasks
            ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
            for (int i = 0; i < numSplits; ++i) {
                splits.add(new FakeInputSplit());
            }

            return splits;
        }

        @Override
        public RecordReader<Text, NullWritable> createRecordReader(
                InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
            // Create a new RandomStackoverflowRecordReader and initialize it
            RandomRecordReader rr = new RandomRecordReader();
            rr.initialize(split, context);
            return rr;
        }

        public class RandomRecordReader extends RecordReader<Text, NullWritable> {
            private Text key = new Text();
            private NullWritable value = NullWritable.get();
            private int numRecordsToCreate = 0;
            private int createdRecords = 0;
            private RowGenbysql rg;
            private final String createtable = "CREATE EXTERNAL TABLE IF NOT EXISTS tbl_data_event_1d_temporary\n" +
                    "  ( record_id                  DECIMAL(23,0)\n" +
                    "  , cdr_id                     STRING\n" +
                    "  , location_code              DECIMAL(7,0)\n" +
                    "  , system_id                  DECIMAL(8,0)\n" +
                    "  , clue_id                    STRING\n" +
                    "  , hit_element                STRING\n" +
                    "  , carrier_code               DECIMAL(2,0)\n" +
                    "  , device_id                  STRING\n" +
                    "  , cap_time                   TIMESTAMP\n" +
                    "  , data_character             STRING\n" +
                    "  , netcell_id                 STRING\n" +
                    "  , client_mac                 STRING\n" +
                    "  , server_mac                 STRING\n" +
                    "  , tunnel_type                STRING\n" +
                    "  , tunnel_ip_client           STRING\n" +
                    "  , tunnel_ip_server           STRING\n" +
                    "  , tunnel_id_client           STRING\n" +
                    "  , tunnel_id_server           STRING\n" +
                    "  , side_one_tunnel_id         STRING\n" +
                    "  , side_two_tunnel_id         STRING\n" +
                    "  , client_ip                  STRING\n" +
                    "  , server_ip                  STRING\n" +
                    "  , trans_protocol             STRING\n" +
                    "  , client_port                DECIMAL(5,0)\n" +
                    "  , server_port                DECIMAL(5,0)\n" +
                    "  , app_protocol               STRING\n" +
                    "  , client_area                DECIMAL(3,0)\n" +
                    "  , server_area                DECIMAL(3,0)\n" +
                    "  , language                   STRING\n" +
                    "  , stype                      STRING\n" +
                    "  , summary                    STRING\n" +
                    "  , file_type                  STRING\n" +
                    "  , filename                   STRING\n" +
                    "  , filesize                   STRING\n" +
                    "  , bill_type                  DECIMAL(6,0)\n" +
                    "  , ori_user_num               STRING\n" +
                    "  , user_num                   STRING\n" +
                    "  , user_imsi                  STRING\n" +
                    "  , user_imei                  STRING\n" +
                    "  , user_belong_area_code      STRING\n" +
                    "  , user_belong_country_code   DECIMAL(10,0)\n" +
                    "  , user_longitude             DECIMAL(20,8)\n" +
                    "  , user_latitude              DECIMAL(20,8)\n" +
                    "  , user_msc                   STRING\n" +
                    "  , user_base_station          STRING\n" +
                    "  , user_curr_area_code        STRING\n" +
                    "  , user_curr_country_code     DECIMAL(10,0)\n" +
                    "  , user_signal_point          STRING\n" +
                    "  , user_ip                    STRING\n" +
                    "  , ori_oppo_num               STRING\n" +
                    "  , oppo_num                   STRING\n" +
                    "  , oppo_imsi                  STRING\n" +
                    "  , oppo_imei                  STRING\n" +
                    "  , oppo_belong_area_code      STRING\n" +
                    "  , oppo_belong_country_code   DECIMAL(10,0)\n" +
                    "  , oppo_longitude             DECIMAL(20,8)\n" +
                    "  , oppo_latitude              DECIMAL(20,8)\n" +
                    "  , oppo_msc                   STRING\n" +
                    "  , oppo_base_station          STRING\n" +
                    "  , oppo_curr_area_code        STRING\n" +
                    "  , oppo_curr_country_code     DECIMAL(10,0)\n" +
                    "  , oppo_signal_point          STRING\n" +
                    "  , oppo_ip                    STRING\n" +
                    "  , ring_time                  TIMESTAMP\n" +
                    "  , call_estab_time            TIMESTAMP\n" +
                    "  , end_time                   TIMESTAMP\n" +
                    "  , call_duration              DECIMAL(10,0)\n" +
                    "  , call_status_code           DECIMAL(3,0)\n" +
                    "  , dtmf                       STRING\n" +
                    "  , ori_other_num              STRING\n" +
                    "  , other_num                  STRING\n" +
                    "  , roam_num                   STRING\n" +
                    "  , send_time                  TIMESTAMP\n" +
                    "  , ori_sms_content            STRING\n" +
                    "  , ori_sms_code               DECIMAL(3,0)\n" +
                    "  , sms_content                STRING\n" +
                    "  , sms_num                    DECIMAL(2,0)\n" +
                    "  , sms_count                  DECIMAL(2,0)\n" +
                    "  , remark                     STRING\n" +
                    "  , content_status             DECIMAL(6,0)\n" +
                    "  , voc_length                 DECIMAL(9,0)\n" +
                    "  , fax_page_count             DECIMAL(6,0)\n" +
                    "  , com_over_cause             DECIMAL(6,0)\n" +
                    "  , roam_type                  DECIMAL(1,0)\n" +
                    "  , sgsn_ad                    STRING\n" +
                    "  , ggsn_ad                    STRING\n" +
                    "  , pdp_ad                     STRING\n" +
                    "  , apn_ni                     STRING\n" +
                    "  , apn_oi                     STRING\n" +
                    "  , card_id                    STRING\n" +
                    "  , time_out                   DECIMAL(1,0)\n" +
                    "  , on_time                    TIMESTAMP\n" +
                    "  , load_id                    DECIMAL(22,0)\n" +
                    "  )\n;";
            private HashMap<String,Double> nullProportion;
            private HashMap<String,Double> distinctProportion;

            public RandomRecordReader(){
                try {
                    nullProportion = new HashMap<>();
                    distinctProportion = new HashMap<>();
                    nullProportion.put("oppo_num",0.5);
                    nullProportion.put("oppo_imsi",0.5);
                    nullProportion.put("oppo_imei",0.5);
                    nullProportion.put("oppo_msc",0.5);

                    distinctProportion.put("clue_id",0.8);
                    distinctProportion.put("netcell_id",0.9);
                    distinctProportion.put("device_id",0.9);
                    distinctProportion.put("user_imsi",0.8);
                    distinctProportion.put("user_imei",0.9);
                    distinctProportion.put("user_num",0.9);
                    distinctProportion.put("card_id",0.9);

                    rg = new RowGenbysql(createtable);
                    rg.setDistinctProportion(distinctProportion);
                    rg.setNullProportion(nullProportion);
                    rg.setColumnGenbysql();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }

            @Override
            public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                this.numRecordsToCreate = taskAttemptContext.getConfiguration().getInt(NUM_RECORDS_PER_TASK, -1);
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {

                if (createdRecords < numRecordsToCreate) {
                    key.set(rg.nextRow());
                    ++createdRecords;
                    return true;
                }else{
                    return false;
                }
            }

            @Override
            public void close(){

            }

            @Override
            public Text getCurrentKey() throws IOException, InterruptedException {
                return key;
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                return (float) createdRecords / (float) numRecordsToCreate;
            }

            @Override
            public NullWritable getCurrentValue() throws IOException, InterruptedException {
                return value;
            }
        }

        /**
         * This class is very empty.
         */
        public static class FakeInputSplit extends InputSplit implements
                Writable {

            @Override
            public void readFields(DataInput arg0) throws IOException {
            }

            @Override
            public void write(DataOutput arg0) throws IOException {
            }

            @Override
            public long getLength() throws IOException, InterruptedException {
                return 0;
            }

            @Override
            public String[] getLocations() throws IOException,
                    InterruptedException {
                return new String[0];
            }
        }

        public static void setNumMapTasks(Job job, int i) {
            job.getConfiguration().setInt(NUM_MAP_TASKS, i);
        }

        public static void setNumRecordPerTask(Job job, int i) {
            job.getConfiguration().setInt(NUM_RECORDS_PER_TASK, i);
        }

    }
    
    public static Properties loadPropertiesFromFile(String file) throws IOException {
        FileInputStream fis = new FileInputStream(file);
        Properties props = new Properties();
        props.load(fis);
        fis.close();
        return props;
    }

    public static void main(String[] args) throws Exception {
        String project_root = System.getProperty("user.dir");
        Properties props =  loadPropertiesFromFile(project_root + "/engines/hive/conf/engineSettings.conf");

        int numMapTasks = Integer.valueOf(props.getProperty("NUM_MAP_TASKS"));
        int numRecordsPerTask = Integer.valueOf(props.getProperty("NUM_RECORDS_PER_TASK"));
	int numReduceTasks = Integer.valueOf(props.getProperty("NUM_REDUCE_TASKS"));
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS",props.getProperty("datagen.filesystem.host"));
//        conf.set("mapreduce.framework.name","yarn");
        Path outputDir = new Path(props.getProperty("datagen.output.directory"));
        FileSystem fs = FileSystem.get(conf);
        fs.delete(outputDir,true);

        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCount.class);
        job.setMapperClass(ShuffleMapper.class);
	job.setPartitionerClass(ShufflePartitioner.class);
//        job.setCombinerClass(ShuffleReducer.class);
        job.setReducerClass(ShuffleReducer.class);
        job.getConfiguration().setInt(NUM_MAP_TASKS, numMapTasks);
        job.getConfiguration().setInt(NUM_RECORDS_PER_TASK, numRecordsPerTask);
//        RandomInputFormat.setNumMapTasks(job, numMapTasks);
//        RandomInputFormat.setNumRecordPerTask(job, numRecordsPerTask);
        job.setNumReduceTasks(numReduceTasks);
        job.setInputFormatClass(RandomInputFormat.class);
        TextOutputFormat.setOutputPath(job, outputDir);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
