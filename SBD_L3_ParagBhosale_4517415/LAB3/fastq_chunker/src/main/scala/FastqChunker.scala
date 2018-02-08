import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.hadoop.io.{LongWritable,Text};
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import scala.collection.mutable.ListBuffer
import scala.collection.mutable._
import org.apache.spark.TaskContext
import org.apache.hadoop.mapreduce.lib.input
//import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.apache.hadoop.mapred.lib._;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration
import java.util.zip.GZIPOutputStream


import java.io._
import java.nio.file.{Paths, Files}

object FastqChunker 
{

	//val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("Chunks.txt"), "UTF-8"))


/*def write2Chunk(index: Int, it: Iterator[String]) 
	{
	
	}
*/


def main(args: Array[String]) 
{
	if (args.size < 3)
	{
		println("Not enough arguments!\nArg1 = number of parallel tasks = number of chunks\nArg2 = input folder\nArg3 = output folder")
		System.exit(1)
	}
	
	val prllTasks = args(0)
	val inputFolder = args(1)
	val outputFolder = args(2)
	
	if (!Files.exists(Paths.get(inputFolder)))
	{
		println("Input folder " + inputFolder + " doesn't exist!")
		System.exit(1)
	}
		 
	// Create output folder if it doesn't already exist
	new File(outputFolder).mkdirs
	
	println("Number of parallel tasks = number of chunks = " + prllTasks + "\nInput folder = " + inputFolder + "\nOutput folder = " + outputFolder)
	
	val conf = new SparkConf().setAppName("DNASeqAnalyzer")
	conf.setMaster("local[" + prllTasks + "]")
	conf.set("spark.cores.max", prllTasks)
	conf.set("spark.default.parallelism","4")
	

	val sc = new SparkContext(conf)
	
	// Comment these two lines if you want to see more verbose messages from Spark
	Logger.getLogger("org").setLevel(Level.OFF);
	Logger.getLogger("akka").setLevel(Level.OFF);
		
	var t0 = System.currentTimeMillis
	
	// Rest of the code goes here
	//====================Reading input files==================================
	val base_RDD_1 = sc.textFile(inputFolder+"/fastq1.fq").zipWithIndex.mapValues(v=>v/4).map{case(a,b)=>(b,a)}
	//read with sc.textfile
	//further index the each record with unique index number
	//now divide the index with 4 to so that 4 lines can now have unique id and flip the position with map
	// this will have format (index,record)
	
	val base_RDD_2 = sc.textFile(inputFolder+"/fastq2.fq").zipWithIndex.mapValues(v=>v/4).map{case(k,v)=>(v,k)}
	//same processing as file 1 
	

	//====================union of RDDs==========================
	val unionRDD=base_RDD_1.union(base_RDD_2).groupByKey.sortByKey(true).flatMap{case(a,b)=>List(b.mkString("\n"))}
	//groupByKey will combine 4 lines into one record
	//then use sortbykey to sort the data according to index
	//sorting will interleve the reads
	//further flatmap will get get only record and discard the index  

	//note: groupByKey is good option when data is less. For big data in size of gb groupByKey will be very slow as compared to reduceByKey or AggregateByKey
	
	unionRDD.foreachPartition { partitionIterator =>
	//write files for each partition
	
	val partitionId = TaskContext.get.partitionId()
	//get partition id for unique file name

	  val zip = new GZIPOutputStream(new FileOutputStream(new File(outputFolder+"/Chunks_"+partitionId+".fq.gz")));
      val bw = new BufferedWriter(new OutputStreamWriter(zip, "UTF-8"));
		//declare zip file format and name for chunks

	while (partitionIterator.hasNext){	
	bw.write(partitionIterator.next+"\n")
	//write records to chunks
	
	}
	bw.close()
	
	}
 

	val et = (System.currentTimeMillis - t0) / 1000 
	println("|Execution time: %d mins %d secs|".format(et/60, et%60))
}
//////////////////////////////////////////////////////////////////////////////
} // End of Class definition
