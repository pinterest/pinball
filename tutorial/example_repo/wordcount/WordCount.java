/**
* Copyright 2015, Pinterest, Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package wordcount;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class WordCount {

  public static class WordSplitter implements Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable oneCount = new IntWritable(1);
    private Text token = new Text();

    @Override
    public void map(LongWritable key,
                    Text value,
                    OutputCollector<Text, IntWritable> output,
                    Reporter reporter) throws IOException {
      StringTokenizer tokenizer = new StringTokenizer(value.toString());
      while (tokenizer.hasMoreTokens()) {
        token.set(tokenizer.nextToken());
        output.collect(token, oneCount);
      }
    }

    @Override
    public void configure(JobConf conf) {
    }

    @Override
    public void close() throws IOException {
    }
   }

  public static class CountAggr implements Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text word,
                       Iterator<IntWritable> values,
                       OutputCollector<Text, IntWritable> output,
                       Reporter reporter) throws IOException {
      int totalCount = 0;
      while (values.hasNext()) {
        totalCount += values.next().get();
      }
      output.collect(word, new IntWritable(totalCount));
    }

    @Override
    public void configure(JobConf conf) {
    }

    @Override
    public void close() throws IOException {
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.out.println("Too few arguments, need to specify input/output path.");
      return;
    }

    JobConf conf = new JobConf(WordCount.class);
    conf.setJobName("pinball-tutorial-wordcount");

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(WordSplitter.class);
    conf.setCombinerClass(CountAggr.class);
    conf.setReducerClass(CountAggr.class);

    for (int i = 0; i < args.length; ++i) {
      System.out.println("argument: " + args[i]);
    }

    // The last 2 arguments are input path and output path.
    FileInputFormat.setInputPaths(conf, new Path(args[args.length - 2]));
    FileOutputFormat.setOutputPath(conf, new Path(args[args.length - 1]));

    JobClient.runJob(conf);
  }
}
