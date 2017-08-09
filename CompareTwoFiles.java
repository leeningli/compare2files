/**
*
*/
package com.youyumen.lee.compare;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
* @author lee
*
*/
public class CompareTwoFiles extends Configured implements Tool
{

    @Override
    public int run(String[] args) throws Exception
    {
        Configuration hadoopConfig = new Configuration();
        Job job = Job.getInstance(hadoopConfig,"CompareTwoFile");
        job.setJobName("CompareTwoFile");
        //String[] fileArgs = new GenericOptionsParser(hadoopConfig,args).getRemainingArgs();
        //job.addCacheFile(new Path(fileArgs[0]).toUri());

        String inputfile = null;
        String outputfile = null;
        inputfile = args[0];
        outputfile = args[1];
        int only_map = 1;
        if(args.length == 3)
        {
        	only_map = 0;
        }
        FileInputFormat.setInputPaths(job, inputfile);
        
        FileOutputFormat.setOutputPath(job, new Path(outputfile));
        job.setJarByClass(CompareTwoFiles.class);
        job.setMapperClass(CompareTwoFileMapper.class);
        job.setNumReduceTasks(only_map);
        job.setReducerClass(CompareTwoFileReducer.class);
        //job.setCombinerClass(CompareTwoFileReducer.class);
        //涉及全文比对的，不能进行部分规约。
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);
        return job.isSuccessful() ? 0 : 1;
    }

    public static class CompareTwoFileMapper extends Mapper<LongWritable,Text,Text,Text>
    {
        private String filename = null;

        @Override
        protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
        {
            String dinggou = null;
            InputSplit inputSplit = context.getInputSplit();
            filename = ((FileSplit)inputSplit).getPath().getName();
            String line = value.toString();
            String[] fields = line.split(" ");
            String telnum = fields[0].trim();//用手机号做key
            if(fields.length > 1)//只有手机号，没有订购关系的，忽略
            {
                dinggou = fields[1].trim() + ";" + filename;//订购关系+文件名做为value
                context.write(new Text(telnum), new Text(dinggou));
            }
        }
    }

    public static class CompareTwoFileReducer extends Reducer<Text,Text,Text,Text>
    {
        @Override
        protected void setup(Context context) throws IOException,InterruptedException
        {
        	file_same = "/lining/compare/output/same.txt";
        	file_more_1 = "/lining/compare/output/more1.txt";
        	file_more_2 = "/lining/compare/output/more2.txt";
        	Configuration conf = new Configuration();
        	fs_same = FileSystem.get(conf);
        	fs_more1 = FileSystem.get(conf);
        	fs_more2 = FileSystem.get(conf);
        	Path path_same = new Path(file_same);
        	Path path_more1 = new Path(file_more_1);
        	Path path_more2 = new Path(file_more_2);
            same = fs_same.create(path_same);
            more1 = fs_more1.create(path_more1);
            more2 = fs_more2.create(path_more2);
        }
        
        @Override
        protected void cleanup(Context context) throws IOException,InterruptedException
        {
        	same.close();
        	more1.close();
        	more2.close();
        	fs_same.close();
        	fs_more1.close();
        	fs_more2.close();
        }
        private String file_same;
        private String file_more_1;
        private String file_more_2;
        Configuration conf = new Configuration();
        private static FSDataOutputStream same = null;//FileSystem.get(conf).create(new Path("/lining/compare/output/same.txt"));
        private static FSDataOutputStream more1 = null;
        private static FSDataOutputStream more2 = null;
        FileSystem fs_same = null;
        FileSystem fs_more1 = null;
        FileSystem fs_more2 = null;

        @Override
        protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException
        {
        	//value: A|B|C;file_1
        	String filename_1 = null;
            String filename_2 = null;
            String dinggou_1 = null;
            String dinggou_2 = null;
        	List<String> dinggous = new ArrayList<String>();
            List<String> tmp_list_1 = new ArrayList<String>();
            List<String> tmp_list_2 = new ArrayList<String>();
            for(Text dinggou : values)
            {
            	String dinggou_value = dinggou.toString().trim();
            	dinggous.add(dinggou_value);              
            }
            if(dinggous.size() == 1)//一方有，一方没有
            {
                filename_1 = dinggous.get(0).split(";")[1];
                String words = filename_1+"\t"+key.toString()+"\t"+dinggous.get(0).split(";")[0]+"\n";
                System.out.println(words);
                more1.write(words.getBytes("UTF-8"));
                String out_key = filename_1+"\t"+key.toString();
                context.write(new Text(out_key), new Text(dinggous.get(0).split(";")[0]));
            }
            else if(dinggous.size() == 2)
            {
            	filename_1 = dinggous.get(0).split(";")[1];
            	filename_2 = dinggous.get(1).split(";")[1];
                dinggou_1 = dinggous.get(0).split(";")[0];
                dinggou_2 = dinggous.get(1).split(";")[0];
                if(dinggou_1.equals(dinggou_2))//双方订购关系完全一致的
                {
                	String out_key = "all_same\t"+key.toString();
                	String words = "all_same\t"+key.toString()+"\t"+dinggou_1+"\n";
                	System.out.println(words);
                    same.write(words.getBytes("UTF-8"));
                    context.write(new Text(out_key), new Text(dinggous.get(0).split(";")[0]));
                }
                else//双方都有，但是具体的订购关系存在差异
                {
                	String a = dinggous.get(0).split(";")[0];
                	String aa[] = a.split("\\|");
                	for(String dinggou_field : aa)
            		{
                		if(dinggou_field.length() != 0)
                		{
                			tmp_list_1.add(dinggou_field);
                		}
            		}
                	String b = dinggous.get(1).split(";")[0];
                	String bb[] = b.split("\\|") ;
                	for(String dinggou_field : bb)
            		{
                		if(dinggou_field.length() != 0)
                			tmp_list_2.add(dinggou_field);
            		}
                	getDiff(context,key.toString(),filename_1,filename_2,tmp_list_1,tmp_list_2);
                }
            }
        }
        
        private static void getDiff(Context context,String telnum,String file1,String file2,List<String> list1, List<String> list2) 
        throws IOException,InterruptedException
        {
             List<String> same_l = new ArrayList<String>();
             List<String> diff_less = new ArrayList<String>();
             List<String> diff_more = new ArrayList<String>();
             List<String> maxList = list1;
             List<String> minList = list2;
             String file_more = file1;
             String file_less = file2;
             String tmp = "";
             if(list2.size() > list1.size())
             {
                 maxList = list2;
                 minList = list1;
                 file_more = file2;
                 file_less = file1;
             }
             Map<String,Integer> map = new HashMap<String,Integer>(maxList.size());
             for (String string : maxList) {
                 map.put(string, 1);
             }
             for (String string : minList) {
                 if(map.get(string)!=null)
                     map.put(string, 2);
                 else
                	 diff_less.add(string);
             }
             for(Map.Entry<String, Integer> entry:map.entrySet())
             {
                 if(entry.getValue()==1)
                 {
                     diff_more.add(entry.getKey());
                 }
                 else if(entry.getValue()==2)
                 {
                	 same_l.add(entry.getKey()); 
                 }
             }
             for(String dinggou : same_l)//双方订购关系部分一致
             {
            	 if(dinggou.length() != 0)
            	 {
            		 if("" == tmp)
                		 tmp = dinggou;
                	 else
                		 tmp = tmp + "|" + dinggou;
            	 }
             }
             String words = "diff()file_same\t"+telnum+"\t"+tmp+"\n";
             System.out.println(words);
             same.write(words.getBytes("UTF-8"));
             if("" != tmp)
            	 context.write(new Text("field_same\t"+telnum), new Text(tmp));
             tmp = "";
             for(String dinggou : diff_less)
             {
            	 if(dinggou.length() != 0)
            	 {
            		 if("" == tmp)
                		 tmp = dinggou;
                	 else
                		 tmp = tmp + "|" + dinggou;
            	 }
             }
             System.out.println(file_less+"\t"+telnum+"\t"+tmp);
             if("" != tmp)
            	 context.write(new Text(file_less+"\t"+telnum), new Text(tmp));
             tmp = "";
             for(String dinggou : diff_more)
             {
            	 if(dinggou.length() != 0)
            	 {
            		 if("" == tmp)
                		 tmp = dinggou;
                	 else
                		 tmp = tmp + "|" + dinggou;
            	 }
             }
             System.out.println(file_more+"\t"+telnum+"\t"+tmp);
             if("" != tmp)
            	 context.write(new Text(file_more+"\t"+telnum), new Text(tmp));
        }
    }
    /**
     * @param args
     */
    public static void main(String[] args) throws Exception
	{
		// TODO Auto-generated method stub
    	if(args.length < 2)
        {
            System.err.println("you have not right inputfile and outpuffile");
            //job执行作业时输入和输出文件的路径
            System.exit(1);
        }
		int isSuccess = 1;
		isSuccess = ToolRunner.run(new CompareTwoFiles(), args);
		System.exit(isSuccess);
	}
}