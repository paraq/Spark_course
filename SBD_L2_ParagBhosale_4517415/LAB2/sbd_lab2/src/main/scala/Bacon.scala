/* Bacon.scala 
export PATH=$PATH:/home/pbhosale/sbd_lab2/sbt/bin
export SPARK_HOME=/data/spark-1.6.1-bin-hadoop2.4/
*/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
//import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.hadoop.io.{LongWritable,Text};
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.commons.lang3.StringUtils
import scala.util.control.Breaks.{break, breakable}
import org.apache.spark.scheduler
import org.apache.spark.scheduler._
import org.apache.spark.storage
import org.apache.spark.storage.{RDDInfo}
import org.apache.spark.scheduler.StageInfo
import scala.collection.mutable.ListBuffer

import java.io._
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Calendar

object Bacon 
{
	 //private[this] var index :Int= 0;
	// private[this] var sub: String="";
	final val KevinBacon = "Bacon, Kevin (I)"	// This is how Kevin Bacon's name appears in the input file for male actors
	val compressRDDs = false

	var counter_M:Int =1;				//counters for male and female actors recquired for output file
	var counter_F:Int =1
	
	var z = new ListBuffer[(String,Int)]		//variable list buffer to update at each iteration

	final val flag="::F::"				//flag to seperate female actors from male actors
	
	

	var write_string_M:String="List of main actors at distance 6:\n";
	var write_string_F:String="List of main actresses at distance 6:\n";

	// SparkListener must log its output in file sparkLog.txt
	val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("sparkLog.txt"), "UTF-8"))
	val outfile = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("actors.txt"), "UTF-8"))	//output file

//================================================================= Different functions ===========================================================
	def getTimeStamp() : String =
	{
		return "[" + new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime()) + "] "
	}
	

	def getyear(s: String) : String =	// This function will return year from given input, getyear("Movie (2015)") output=2015, this will help to filter out the movies
	{
		return s.substring(s.lastIndexOf("(")+1,s.lastIndexOf("(")+5) //substring will cut year from given input,it will search last index of ("(") +1 till next 4 
	}


	def getmovie(s: String) : String =	// This function will return appropriate movie format till (YYYY), it will allow other columns before (YYYY). Thats why, in the getyear function  
	{					// lastIndexOf "(" is used so that it will find (YYYY) format. getmovie(movie(en)(2005) [himself] <20>)= movie(en)(2005)
						// another advantage of this function is that it will remove \n present in the records.
		var sub: String="";		//initialize sub to empty and index to 0 
		var index :Int= 0; 


		breakable{ while(true)		//while loop will run untill it finds (YYYY) format. 
		{ sub= s.substring(s.indexOf("(",index)+1,s.indexOf("(",index)+5) // cut the string from first appearance of ( till next 4 chactersters starting from index(which is initialzed to 0)
					
			if(StringUtils.isNumeric(sub) || sub =="????") // check is that substring is a number or "????"
			{ 	index=s.indexOf("(",index)+1;          // update index
				break;				      // as number or "????" is found while loop is break
			}
		index=s.indexOf("(",index)+1;				//update index for next loop, this means YYYY is not found,proceed further and start searching for next "("
		} }

	
	return s.substring(0,s.indexOf(")",index)+1)			//return the substring up to (YYYY)
	}

	def write2Log(a: (String)) //function to write final output to file actors.txt
	{
			
			if((a).contains(flag))	//separate female actresses from male actresses
			{  
				write_string_F=write_string_F+ counter_F +". "+ a.replace(flag,"")+"\n" //according to output file first male actors are written and then female actors
													//so we will write female records to a string and write it to the file once male records are 														//written. In this way we do not need to read RDD again t write female records
				
				
				counter_F = counter_F +1;						//increment the counter for female records
							
			}else			// else will have male records
				{
				
				outfile.write(counter_M +". "+ a+"\n")					//write male actors records directly to the output file
				counter_M = counter_M +1;						//increment the counter for male records
				
				}
	}


	def convert_array (a:(String,Int))	//function to append tuples value to a variable z for the iterations
	{
		z.append(a)
		
		
	}
//===============================================================================================================================================================

	def main(args: Array[String]) 
	{
		val cores = args(0)			// Number of cores to use
		val inputFileM = args(1)		// Path of input file for male actors
		val inputFileF = args(2)		// Path of input file for female actors (actresses)
		
		
		val conf = new SparkConf().setAppName("Kevin Bacon app")
		conf.setMaster("local[" + cores + "]")
		conf.set("spark.cores.max", cores)
		conf.set("spark.rdd.compress",compressRDDs.toString)
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		val sc = new SparkContext(conf)

		val conf_1= new org.apache.hadoop.mapreduce.Job().getConfiguration
		conf_1.set("textinputformat.record.delimiter", "\n\n")			//read the records separated by \n\n delimiter
		
//========================================================== Add SparkListener's code here ========================================================================
		sc.addSparkListener(new SparkListener() {
		override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) { //whenever a stage for a RDD is completed SparkListener will log the data 
 
  		val stageInfo = stageCompleted.stageInfo;				    //using rddInfos inside stageinfo class
  		val rddInfos = stageInfo.rddInfos;
 
  		rddInfos.foreach(row => {
		if(row.isCached) {		// check if RDD is cached  
		 println("RDD info is logging for " +row.name+ "..... "); //print message
		bw.write( getTimeStamp() +":"+ row.name+ "  memSize:" + (row.memSize/(1024*1024))+"(MB)  diskSize:"+(row.diskSize/(1024*1024))+"(MB)  numPartitions:"+row.numPartitions+"\n")}
      										//log info about cached RDD. 
										//row.name=> name of RDD set by setname
										//row.memSize=>size of RDD on RAM
										//row.diskSize=>size of RDD on disk
										//row.numPartitions=> number of partitions for RDD
   		 })
  
		}

		});
		
//=============================================================================================================================================================

		println("Number of cores: " + args(0))
		println("Input files: " + inputFileM + " and " + inputFileF)
		
		// Comment these two lines if you want to see more verbose messages from Spark
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		var t0 = System.currentTimeMillis
		
		// Add your main code here

//===========================================================Reading file and converting to required data=========================================================================

		//files are read using newAPIHadoopFile with TextInputFormat format which will give (LongWritable,text) format LongWritable will be index of the new record separated by "\n\n"
		// as declared in the conf_1 
		val actors_in = sc.newAPIHadoopFile(inputFileM,classOf[TextInputFormat],classOf[LongWritable],classOf[Text],conf_1).map{ case (_, v) => v.toString }
		val actors_in_F = sc.newAPIHadoopFile(inputFileF,classOf[TextInputFormat],classOf[LongWritable],classOf[Text],conf_1).map{ case (_, v) => v.toString }
		println("Reading from file at"+getTimeStamp())

		//both input RDDs are converted to (actors,list<movies>) by using whitespace as seaparator ann 1st value will be actors and others will be sequence of movies
		val usgPairRDD: RDD[(String, Seq[String])] = actors_in.map(_.split("\t") match {case Array(x, xs @ _*) => (x, xs)})
		val usgPairRDD_F: RDD[(String, Seq[String])] = actors_in_F.map(_.split("\t") match {case Array(x, xs @ _*) => (x, xs)})
		
		//flatten the data using flatmap to yield format (actor,movie) 
		// further empty movies records are filtered using first filter
		// second filter will fiter out television series contains " in the record
		val pair = usgPairRDD.flatMap {case (k, xs) => for ((d) <- xs) yield (k, d)}.filter{case (a,b) => b!=""}.filter{case (a,b) => b.contains("\"")==false}
		val pair_F = usgPairRDD_F.flatMap {case (k, xs) => for ((d) <- xs) yield (k, d)}.filter{case (a,b) => b!=""}.filter{case (a,b) => b.contains("\"")==false}

		
		//Get appropriate name for the movie in format movie(YYYY) for both male and female actresses
		val pair_1=pair.map{case (a,b) => (a,getmovie(b))}
		val pair_1_F=pair_F.map{case (a,b) => (a+flag,getmovie(b))} //add flag for female actresses 
		
		//union both male and female actresses
		val pair_all=pair_1.union(pair_1_F)
		
		//Get only latest movies from 2011 and filter out movies with unknown year
		val latest= pair_all.filter{case(a,b)=> getyear(b) != "????"}.filter{case(a,b)=> getyear(b).toInt >= 2011}
		latest.setName("latest ")

		//Get total number of movies by applying distinct. Map will get (actor,movies)=> (movies)
		val movie_count=latest.map{case(a,b)=>(b)}.distinct.count

		// To get co-actors pair we should self join latest RDD with movie as the key. First map will set movie as the key
		//join will give output (movies,(actor,actor)). further map will get output (actor,actor) coactors tuple. Note that this will have values like (movie,(Kevin,Kevin)) because of selfjoin
		//filter such records using filter 
		val joined=latest.map{case (a,b)=>(b,a)}.join(latest.map{case (a,b)=>(b,a)}).map{case(a,(b,c))=>(b,c)}.filter{case(a,b)=> a!=b}

		//flip the values so that they can used in join transformation in the iteration
		val joined_rev=joined.map{case(a,b)=>(b,(a))}//.persist()
		joined_rev.setName("Self join reverse RDD")

		//Create distance RDD using map to get only actors and use .distinct to get distinct values.This is RDD of unique actor name
		//second map will add distance in such case if actor is kevin bacon distance =0 , otherwise infinite(9999)
		val dis_rdd= joined.map{case(a,b)=> (a) }.distinct.map{case(a) => (a, if (a==KevinBacon) 0 else 9999)}

		//count total number of actors, male as well as female actors
		val total=dis_rdd.count.toInt
		val total_F=dis_rdd.filter{case(a,b)=>a.contains(flag)}.count.toInt
		val total_M=(total-total_F).toInt

		//log data to output file
		outfile.write("Total number of actors ="+total+", out of which "+ (total-total_F)+" ("+ (((total-total_F).toFloat/total)*100)+") are males while "+ total_F +" ("+((total_F.toFloat/total)*100)+") are females\n")		
		outfile.write("Total number of movies = "+movie_count+"\n")
		
		//load distance RDD into variable z of list buffer type for the first iteration 
		dis_rdd.collect.foreach( rdd=> convert_array(rdd))
	
		val previous_total=0
		var previous_F:Int=0
		var previous_M:Int=0

//=================================================================================iterations========================================================================================
		for (i <- 1 to 6) {
		val distData = sc.parallelize(z)			//Load z array into distance RDD (actor,distance):: this RDD will be updated at each iteration
		distData.setName("Distance Iteration RDD"+i)		// set name for the RDD according to the distance

   		val redu_1= joined_rev.join(distData).map{case(a,(b,c))=>(b,c.toInt)}.reduceByKey(math.min(_ , _)).map{case (a,b)=> (a,b+1)}.persist()
									//join (actor,actor1) with (actor,distance) => (actor,(actor1,distance)) here actor is co-actor of actor with distance 
									//map transformation will give only (actor1,distance) with actors having mutiple distances according to co actors
									//reduceBykey with actor1 to get minimun distance and last map transformation will increase the distance by 1
		redu_1.setName("join  RDD for "+i)
		//redu_1.coalesce(1,true).saveAsTextFile("redu_1.txt")
		val count_1= redu_1.filter{case(a,b)=> b==i }.count.toInt 			//find count of the total actors with distance 1
		val count_1_F= redu_1.filter{case(a,b)=> b==i && a.contains(flag)}.count.toInt //find count of the female actors with distance 1
		val count_1_M=count_1-count_1_F							//find count of the male actors with distance 1
		
	
outfile.write("There are "+(count_1_M-previous_M)+" ("+(((count_1_M-previous_M).toFloat/total_M)*100)+") actors and "+(count_1_F-previous_F)+" ("+(((count_1_F-previous_F).toFloat/total_F)*100)+") actresses at distance "+i+"\n")		// write logs to actors.txt
		z.clear			//clear listbuffer for this iteration
		//distData.unpersist()
		redu_1.collect.foreach( rdd=> convert_array(rdd)) //load variable z with updated distance RDD 	
		previous_F=count_1_F				  //assign variabes with this iteration to calculate count for the next iteration  
		previous_M=count_1_M			
  

		if (i==6)
		{	
	val dist_6=distData.join(redu_1).filter{case(a,(b,c))=> b>c}.map{case(a,(b,c))=>(a,b)}.sortBy(_._1).map{case(a,b)=>(a)}
								//join latest iteration with 5 th iteration to get (actor,distance at 5,distance at 6)
								//actor at dist 6 will have distance at 6 less than that of at 5(infinite), filter the data using same condition 

								//log the data into output file
	outfile.write("\nTotal number of actors from distance 1 to 6 = "+(count_1)+", Ratio="+((count_1.toFloat)/total)+"\n")
	outfile.write("Total number of male actors from distance 1 to 6 = "+(count_1_M)+", Ratio="+((count_1_M.toFloat)/total_M)+"\n")
	outfile.write("Total number of female actors from distance 1 to 6 = "+(count_1_F)+", Ratio="+((count_1_F.toFloat)/total_F)+"\n\n")
		outfile.write(write_string_M)			//write write_string_M string to file
		dist_6.collect.foreach(rdd => write2Log(rdd))   //call write2lof foreach tuple	
		outfile.write("\n"+write_string_F);		//write actresses output to the file
		}



			}


//=====================================================================================================================================================================================
		

		sc.stop()					//close the context
		bw.close()					//close files
		outfile.close()
		
		val et = (System.currentTimeMillis - t0) / 1000 
		println("{Time taken = %d mins %d secs}".format(et/60, et%60)) //record the execution time
	} 
}
