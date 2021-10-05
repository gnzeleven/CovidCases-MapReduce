import java.lang.Iterable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import scala.collection.JavaConverters._

class CovidCases

object CovidCases {

  class CovidCasesMapper extends Mapper[Object, Text, Text, IntWritable] {

    val state = new Text()
    val caseCount = new IntWritable()

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context) : Unit = {
      val line = value.toString().split(",")
      state.set(line(1))
      caseCount.set(line(3).toInt)
      context.write(state, caseCount)
    }
  }

  class CovidCasesReducer extends Reducer[Text, IntWritable, Text, IntWritable] {

    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context) : Unit = {
      var sum = values.asScala.foldLeft(0)(_ + _.get)
      context.write(key, new IntWritable(sum))
    }
  }

  def main(args: Array[String]) : Unit = {
    val configuration = new Configuration
    val job = Job.getInstance(configuration, "Covid Cases")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[CovidCasesMapper])
    job.setReducerClass(classOf[CovidCasesReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
//    job.setInputFormatClass(classOf[TextInputFormat])
//    job.setOutputFormatClass(classOf[TextOutputFormat])
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    System.exit(if(job.waitForCompletion(true)) 0 else 1)
  }
}
