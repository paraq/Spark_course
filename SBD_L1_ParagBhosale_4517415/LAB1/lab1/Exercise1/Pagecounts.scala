import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io._
import java.util.Locale
import org.apache.commons.lang3.StringUtils

object Pagecounts 
{
	// Gets Language's name from its code
	def getLangName(code: String) : String =
	{
		new Locale(code).getDisplayLanguage(Locale.ENGLISH)
	}
	
	def main(args: Array[String]) 
	{
		val inputFile = args(0) // Get input file's name from this command line argument
		val conf = new SparkConf().setAppName("Pagecounts")
		val sc = new SparkContext(conf)
		
				
	
		// Uncomment these two lines if you want to see a less verbose messages from Spark
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		val t0 = System.currentTimeMillis
		
		// Add your code here
			
		val page = sc.textFile(inputFile)
		//read the file given by command input	
		
		//=====================================================================Transformations============================================================================

		val map1=(page).map(line => line.split(" ")).map(line => ((StringUtils.substringBefore(line(0), ".")),(StringUtils.substringBefore(line(0), "."),line(2).toInt,line(1),line(2).toInt)))
		/* First map tranformation will split each row by " "(space delimitor) and corresponding line(0) means data from first column.
		   Second Map will convert data into required key-value tuple format of (langcode),(langcode,viewcount,pagetitle,viewcount) => langcode as the key
		   we will reduce values as (languagename,TotalViewsInThatLang,MostVisitedPageInThatLang,ViewsOfThatPage)
		   StringUtils.substringBefore(line(0), ".") is used to get text before '.'. StringUtils.substringBefore("fr.co", ".")="fr"
		   we need to convert line(2) i.e. viewcount to int for applying addtion and Max functions.
				
		*/
		//val map2= map1.filter()

		val outRDD1=map1.reduceByKey((x,y) => (if (y._3 == y._1) x._1 else getLangName(y._1), if (y._3 == y._1) x._2 else x._2 + y._2,if (y._3 == y._1){x._3} else {if (x._4 > y._4) x._3 else y._3 },if (y._3 == y._1) x._4 else math.max(x._4, y._4)))

		
		/*
		we need to apply reduce functions on the (langcode,viewcount,pagetitle,viewcount) 
		langname =>  if (y._3 == y._1) x._1 else getLangName(y._1) // if condition is used to check whether pagetitle is same as language code
									      if they are same use previous value x._1 (this condition is applied to each reduce functions to filter out records)
									      else condtion when pagetitle!=langcode gives language name by using fuction getLangName

 		TotalViewsInThatLang=> if (y._3 == y._1) x._2 else x._2 + y._2 // if condition to filter out records and else condtion to add viewcount to get total viewcount in that language

		MostVisitedPageInThatLang => if (y._3 == y._1){x._3} else {if (x._4 > y._4) x._3 else y._3} // if condition in else loop will check if previous page has more views than the current
														page if yes, select previous page else select current page
		ViewsOfThatPage => if (y._3 == y._1) x._4 else math.max(x._4, y._4) // else condition will return maximum value between current and previous viewcount, this will give maximum 
											viewcount of MostVisitedPageInThatLang 

		*/
		val outRDD=outRDD1.map{case (a,(b,c,d,e))=>(b,a,c,d,e)}.sortBy(_._3,false)// can transform be used in this case?
		// map will convert present tuple (langcode,(langname,TotalViewsInThatLang,MostVisitedPageInThatLang,ViewsOfThatPage)) to required output format
		// (langname,langcode,TotalViewsInThatLang,MostVisitedPageInThatLang,ViewsOfThatPage)
		// sortBy(_._3,false) will sort according to TotalViewsInThatLang,false for descending order 

		// ======================================================================Writing to the file========================================================================== 
		val file = new FileOutputStream("part1.txt");// name of the output file
		val bw = new BufferedWriter(new OutputStreamWriter(file, "UTF-8"))
		bw.write("Language,Language-code,TotalViewsInThatLang,MostVisitedPageInThatLang,ViewsOfThatPage\n")
		bw.write(outRDD.collect.mkString("\n"))// convert to string for writing to a file 
		bw.close
				
		//=======================================================================Runtime display============================================================================
		
		val et = (System.currentTimeMillis - t0) / 1000
		System.err.println("Done!\nTime taken = %d mins %d secs".format(et / 60, et % 60))
	}
}

