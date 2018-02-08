import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import java.io._
import java.util.Locale
import org.apache.tika.language.LanguageIdentifier
import java.util.regex.Matcher
import java.util.regex.Pattern

object Twitterstats
{ 
	var firstTime = true
	var t0: Long = 0
	val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("twitterLog.txt"), "UTF-8"))
	
	// This function will be called periodically after each 5 seconds to log the output. 
	// Elements of a are of type (lang, totalRetweetsInThatLang, idOfOriginalTweet, maxRetweetCount, minRetweetCount,RetweetCount,text)
	def write2Log(a: Array[(String, Long, Long, Long, Long, Long,String)])
	{ // i have changed the write2lof function according to my convenience
		if (firstTime)
		{
			bw.write("Seconds,Language,Language-code,TotalRetweetsInThatLang,IDOfTweet,MaxRetweetCount,MinRetweetCount,RetweetCount,Text\n")
			t0 = System.currentTimeMillis
			firstTime = false
		}
		else
		{
			val seconds = (System.currentTimeMillis - t0) / 1000
			
			if (seconds < 60)
			{
				println("Elapsed time = " + seconds + " seconds. Logging will be started after 60 seconds.")
				return
			}
			
			println("Logging the output to the log file\nElapsed time = " + seconds + " seconds\n-----------------------------------")
			
			for(i <-0 until a.size)
			{
				val langCode = a(i)._1
				val lang = getLangName(langCode)
				val totalRetweetsInThatLang = a(i)._2
				val id = a(i)._3
				val textStr = a(i)._7.replaceAll("\\r|\\n", " ")	//removing new line characters from the text
				val maxRetweetCount = a(i)._4
				val minRetweetCount = a(i)._5
				val retweetCount = a(i)._6
				
				bw.write("(" + seconds + ")," + lang + "," + langCode + "," + totalRetweetsInThatLang + "," + id + "," + 
					maxRetweetCount + "," + minRetweetCount + "," + retweetCount + "," + textStr + "\n")
			}
		}
	}
  
	// Pass the text of the retweet to this function to get the Language (in two letter code form) of that text.
	def getLang(s: String) : String =
	{
		val inputStr = s.replaceFirst("RT", "").replaceAll("@\\p{L}+", "").replaceAll("https?://\\S+\\s?", "")
		var langCode = new LanguageIdentifier(inputStr).getLanguage
		
		// Detect if japanese
		var pat = Pattern.compile("\\p{InHiragana}") 
		var m = pat.matcher(inputStr)
		if (langCode == "lt" && m.find)
			langCode = "ja"
		// Detect if korean
		pat = Pattern.compile("\\p{IsHangul}");
		m = pat.matcher(inputStr)
		if (langCode == "lt" && m.find)
			langCode = "ko"
		
		return langCode
	}
  
	// Gets Language's name from its code
	def getLangName(code: String) : String =
	{
		return new Locale(code).getDisplayLanguage(Locale.ENGLISH)
	}

	val reduce = (x: (String,Long,Long,String), y: (String,Long,Long,String)) => {
   	//
   	( y._1,math.max(x._2, y._2),math.min(x._3, y._3),y._4)	
   	/*	reduce function will give following output from current and previous values of (langcode,langname,Maxretweetcount,Minretweetcount,text)
	langcode,langname and text=> function will select respective current values
	 Maxretweetcount =>  math.max(x._3, y._3) , it will select grater value between current and previous retweetcount
	 Minretweetcount =>  math.min(x._4, y._4) , it will select smaller value between current and previous retweetcount
	 */
	
	}
  
	def main(args: Array[String]) 
	{
		// Configure Twitter credentials
		val apiKey = " MnVNvmaFsXDsynk4itBhQYIkB"
		val apiSecret = "JFzxiXxNskhiE7D8t9Pn8qQbraDu8Qz10f2auhXIYEafqsOTXs"
		val accessToken = " 784442412043296768-zrWqfEIy4zXF9aij0Tc9GaDo2Kiswiu"
		val accessTokenSecret = " 8XaAqR9LOoJh7sKtkxqcNitdrA29qb6zOdaMBDKWNiaOU"
		//authentication of twitter application
		Helper.configureTwitterCredentials(apiKey, apiSecret, accessToken, accessTokenSecret)
		
		val ssc = new StreamingContext(new SparkConf(), Seconds(5))
		val tweets = TwitterUtils.createStream(ssc, None)
		
		// Add your code here
		//======================================================Transformations on Dstream=========================================

		val filter_re = tweets.filter(status => status.isRetweet())
		//filter retweeted tweets .isRetweet is true if retweet
		val statuses = filter_re.map(line => ((getLang(line.getText())),(line.getRetweetedStatus().getId(),line.getRetweetedStatus().getRetweetCount(),line.getRetweetedStatus().getRetweetCount(),line.getText())))
		/* Map is needed to get desired tuple format from tweets , statuses will be (langcode,(originalID,retweetcount,retweetcount,text)) , here langcode is the key
		  getLang => give language code of the tweet 
		  getRetweetedStatus => to get information about original tweet further getID is used to get original tweet ID.
		  etRetweetedStatus().getRetweetCount() => to get retweet count of the original tweet.
		  getText => to get text of the tweet
		*/

		val statuses_reduce=statuses.map{case (co,(id,maxrecount,minrecount,text))=> (id,(co,maxrecount,minrecount,text))}.reduceByKeyAndWindow(reduce,Seconds(60), Seconds(5)).map{case (c,(a,d,e,f)) => (a,c,d,e,d-e+1,f)}.transform(rdd => rdd.sortBy(_._5,false))
		/* first Map will convert the format in such way we get tweet as the key. This is needed to get max and min of retweet count of that id
		   so output of first Map => (ID,(langcode,retweetcount,retweetcount,text))
		   on above format reduceByKeyAndWindow is applied with tweetID as the key using reduce function and window of 60 sec and interval of 5 sec.
		   After reduction function we will get max and min of retweetcount of that particular tweetID
		   Second Map is used to get format (langcode,tweetID,Maxretweetcount,Minretweetcount,Totalretweetcount,text) where Totalretweetcount=Maxretweetcount-Minretweetcount+1
		   Then sortby is applied to get data in descending order with respect to Totalretweetcount
		*/

		val max_count_lang= statuses_reduce.map{case (a,c,d,e,total,f) => (a,(total))}
		/* Now we need to calculate totalretweetcountoflang. For this we need output of above transformation 
		   Map is applied to get (langcode,Totalretweetcount) such that reduce is apllied on langcode.
		*/
		//val counts = max_count_lang.reduceByKeyAndWindow(_ + _,  _ - _,Seconds(60), Seconds(5))
		val counts = max_count_lang.reduceByKey(_ + _)
		//reducebykey is applied on key langcode,  _+_ is used to get totalretweetcountofthatlanguage.


		val joined = counts.join(statuses_reduce.map{case(a,c,d,e,total,f)=>(a,(c,d,e,total,f))}).map{case(a,(b,(d,e,f,g,h)))=> (a,b,d,e,f,g,h)}.transform(rdd => rdd.sortBy(_._2,false))
		/* To get desired output we must join counts and statuses_reduce. But first Map is applied to statuses_reduce to get key value pair. Here key is language code
			statuses_reduce => (langcode,(tweetID,Maxretweetcount,Minretweetcount,Totalretweetcount,text)) is joined (key=langcode) to 
			counts => (langcode,totalretweetcountofthatlanguage) 
			output of join will be Array[(langcode,(totalretweetcountofthatlanguage,(tweetID,Maxretweetcount,Minretweetcount,Totalretweetcount,text)))]
			Map is then applied to get desired array of output format => (sec,langcode,totalretweetcountofthatlanguage,tweetID,Maxretweetcount,Minretweetcount,Totalretweetcount,text)
			sortBy(_._2,false) is used to arrange data in descending order wrt totalretweetcountofthatlanguage
		*/
		//statuses_reduce.print()	// used for debugging
		//counts.print()   		// used for debugging 
		 
		// If elements of RDDs of outDStream aren't of type (lang, totalRetweetsInThatLang, idOfOriginalTweet, text, maxRetweetCount, minRetweetCount),
		//	then you must change the write2Log function accordingly.


		joined.foreachRDD(rdd => write2Log(rdd.collect)) 
	
		new java.io.File("cpdir").mkdirs // make new directory for checkpoint
		ssc.checkpoint("cpdir")// to enable periodic RDD checkpointing because reduceByKeyAndWindow is used in this application.Checkpoint is used get enough data so that storage can recover
					// from failures
		ssc.start()
		ssc.awaitTermination()
	}
}

