/* 
 * Copyright (c) 2015-2016 TU Delft, The Netherlands.
 * All rights reserved.
 * 


 * You can redistribute this file and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the
 * Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This file is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * Authors: Hamid Mushtaq
 *
*/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level

import sys.process._
import scala.sys.process.Process
import java.nio.file.{Paths, Files}
import java.io._

import tudelft.utils.ChromosomeRange
import tudelft.utils.DictParser
import tudelft.utils.Configuration
import tudelft.utils.SAMRecordIterator
import org.apache.spark.RangePartitioner
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import org.apache.commons.lang3.StringUtils

import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Calendar

import htsjdk.samtools._


object DNASeqAnalyzer 
{
final val MemString = "-Xmx5120m" 
final val RefFileName = "ucsc.hg19.fasta"
final val SnpFileName = "dbsnp_138.hg19.vcf"
final val ExomeFileName = "gcat_set_025.bed"

//////////////////////////////////////////////////////////////////////////////
	def getTimeStamp() : String =
	{
		return "[" + new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime()) + "] "
	}

def bwaRun (x: String, config: Configuration) : 
	Array[(Int, SAMRecord)] = 
{
//===============================Get required directories=================================
	val tmpFolder = config.getTmpFolder
	val toolsFolder = config.getToolsFolder
	val refFolder = config.getRefFolder
	val numOfThreads = config.getNumThreads
	val inputFolder = config.getInputFolder
	val fastqChunk= config.getInputFolder + x
	val outFileName = config.getTmpFolder + x + ".sam"
	val outputFolder = config.getOutputFolder
//==============================================================================================
	
	val logfile= outputFolder+"/log/bwa/"+StringUtils.substringBefore(x,".fq.gz")+"_log.txt"
	//log file for bwa 
	val logbwa = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logfile), "UTF-8"))
	val chunkin= StringUtils.substringBefore(fastqChunk,".gz")
	
//=========================bwa mem algorithm===================================================
	logbwa.write(getTimeStamp() +": gunzip input chunk \n")
	Seq("gunzip",inputFolder+x)!
	//unzip .gz chunk files
	
	logbwa.write(getTimeStamp() +": bwa mem "+refFolder+RefFileName+" -p -t "+numOfThreads+" "+chunkin + " >" +outFileName+ "\n" )
	//logging
	Seq(toolsFolder+"bwa","mem", refFolder+RefFileName, "-p", "-t" ,numOfThreads, chunkin) #> new File(outFileName)!// Your command
	//this will create unix command and execute=>  bwa mem refFolder/RefFileName -p -t numOfThreads chunkin > outFileName	
	//bwa mem == aligment algorithm
	//refFile == reference chromosome file against which input chunks will be mapped/aligned
	//-p == interleaved RAW records which are input chunks
	//-t == allow threads per bwa task
	// numOfThreads number of threads
	// outFileName == output SAM file name

//=========================Getting key value pairs===========================================
	val bwaKeyValues = new BWAKeyValues(outFileName)
	bwaKeyValues.parseSam()
	val kvPairs: Array[(Int, SAMRecord)] = bwaKeyValues.getKeyValuePairs()
	
	logbwa.write(getTimeStamp()+": KV value pairs:"+kvPairs.length)
	logbwa.write(getTimeStamp()+": KV value pairs:"+kvPairs.length)
	logbwa.close()
	
	Files.deleteIfExists(Paths.get(outFileName))// Delete the temporary files
	
	// Delete the temporary files
	return kvPairs 
}
	 
def writeToBAM(fileName: String, samRecordsSorted: Array[SAMRecord], config: Configuration) : ChromosomeRange = 
{
	println("writing BAM file::"+fileName)	
	val header = new SAMFileHeader()
	header.setSequenceDictionary(config.getDict())
	val outHeader = header.clone()
	outHeader.setSortOrder(SAMFileHeader.SortOrder.coordinate);
	val factory = new SAMFileWriterFactory();
	val writer = factory.makeBAMWriter(outHeader, true, new File(fileName));
	
	val r = new ChromosomeRange()
	val input = new SAMRecordIterator(samRecordsSorted, header, r)
	while(input.hasNext()) 
	{
		val sam = input.next()
		writer.addAlignment(sam);
	}
	writer.close();
	
	return r
}

def variantCall (chrRegion: Int, samRecordsSorted: Array[SAMRecord], config: Configuration) : 
	Array[(Integer, (Integer, String))] = 
{	
//===========================Set directories and file names============================================================
	val tmpFolder = config.getTmpFolder
	val toolsFolder = config.getToolsFolder
	val refFolder = config.getRefFolder
	val numOfThreads = config.getNumThreads
	val outputFolder = config.getOutputFolder
	
	val logfile= outputFolder+"/log/vc/chunk"+chrRegion+"_vari_log.txt"
	val logvari = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logfile), "UTF-8"))
	//log file for variant call 

	// Following is shown how each tool is called. Replace the X in regionX with the chromosome region number (chrRegion). 
	// 	You would have to create the command strings (for running jar files) and then execute them using the Scala's process package. More 
	// 	help about Scala's process package can be found at http://www.scala-lang.org/api/current/index.html#scala.sys.process.package.
	//	Note that MemString here is -Xmx6144m, and already defined as a constant variable above, and so are reference files' names.



	// SAM records should be sorted by this point
	//============================================Variant calling pipeline=======================================
	val regP1Bam = tmpFolder + "region" + chrRegion + "-p1.bam"
	val chrRange = writeToBAM(regP1Bam, samRecordsSorted, config)
	
	//==============================Picard preprocessing========================================
	
	val regP2Bam = tmpFolder + "region" + chrRegion + "-p2.bam"	
	logvari.write(getTimeStamp()+": java MemString -jar "+toolsFolder + "CleanSam.jar INPUT=" + regP1Bam+" OUTPUT=" + regP2Bam+"\n")
	Seq("java",MemString,"-jar",toolsFolder + "CleanSam.jar","INPUT=" + regP1Bam,"OUTPUT=" + regP2Bam)!

	val regP3Bam = tmpFolder + "region" + chrRegion + "-p3.bam"
	val regP3MetTxt = tmpFolder + "region" + chrRegion + "-p3-metrics.txt"
	logvari.write(getTimeStamp()+": java MemString -jar "+toolsFolder+"MarkDuplicates.jar INPUT=" + regP2Bam+" OUTPUT=" + regP3Bam+" METRICS_FILE=" + regP3MetTxt+"\n") 
	Seq("java",MemString,"-jar",toolsFolder+"MarkDuplicates.jar","INPUT=" + regP2Bam,"OUTPUT=" + regP3Bam,"METRICS_FILE=" + regP3MetTxt)!	

	val regBam = tmpFolder + "region" + chrRegion + ".bam"
	logvari.write(getTimeStamp()+": java MemString -jar "+toolsFolder+"AddOrReplaceReadGroups.jar INPUT=" + regP3Bam+" OUTPUT=" + regBam+" RGID=GROUP1 RGLB=LIB1 RGPL=ILLUMINA RGPU=UNITI RGSM=SAMPLE1 \n")
	Seq("java",MemString,"-jar",toolsFolder+"AddOrReplaceReadGroups.jar","INPUT=" + regP3Bam,"OUTPUT=" + regBam,
	    "RGID=GROUP1","RGLB=LIB1","RGPL=ILLUMINA","RGPU=UNITI","RGSM=SAMPLE1")!
	val temp=null //do nothing 
	logvari.write(getTimeStamp()+"java MemString -jar toolsFolder/BuildBamIndex.jar INPUT=tmpFolder/regionX.bam \n")
	Seq("java",MemString,"-jar",toolsFolder + "BuildBamIndex.jar","INPUT=" + regBam)!

	// Delete tmpFolder/regionX-p1.bam, tmpFolder/regionX-p2.bam, tmpFolder/regionX-p3.bam and tmpFolder/regionX-p3-metrics.txt	
	Files.deleteIfExists(Paths.get(regP1Bam))
	Files.deleteIfExists(Paths.get(regP2Bam))
	Files.deleteIfExists(Paths.get(regP3Bam))
	Files.deleteIfExists(Paths.get(regP3MetTxt))

	//============================================Make region file =============================
	val tmpBed = tmpFolder + "tmp" + chrRegion + ".bed"
	val bedBed = tmpFolder + "bed" + chrRegion + ".bed"
	val tmpBedFile = new File(tmpBed)
	chrRange.writeToBedRegionFile(tmpBedFile.getAbsolutePath())
	logvari.write(getTimeStamp()+": toolsFolder/bedtools intersect -a refFolder/ExomeFileName -b tmpFolder/tmpX.bed -header > tmpFolder/bedX.bed \n")
	Seq(toolsFolder + "bedtools","intersect","-a",refFolder + ExomeFileName,"-b",tmpBed,"-header") #> new File(bedBed)!

	// Delete tmpFolder/tmpX.bed
	Files.deleteIfExists(Paths.get(tmpBed))

	//==========================================Indel Realignment=================================== 
	val GATKJar = toolsFolder + "GenomeAnalysisTK.jar"
	val regInt = tmpFolder + "region" + chrRegion + ".intervals"
	val refFile = refFolder + RefFileName
	logvari.write(getTimeStamp()+": java MemString -jar toolsFolder/GenomeAnalysisTK.jar -T RealignerTargetCreator -nt numOfThreads -R refFolder/RefFileName -I tmpFolder/regionX.bam -o tmpFolder/regionX.intervals -L tmpFolder/bedX.bed \n")
	Seq("java",MemString,"-jar",GATKJar,"-T","RealignerTargetCreator","-nt",numOfThreads,"-R",refFile,"-I",regBam,"-o",regInt,"-L",bedBed)!

	val reg2Bam = tmpFolder + "region" + chrRegion + "-2.bam"
	logvari.write(getTimeStamp()+": java MemString -jar toolsFolder/GenomeAnalysisTK.jar -T IndelRealigner -R refFolder/RefFileName -I tmpFolder/regionX.bam -targetIntervals tmpFolder/regionX.intervals -o tmpFolder/regionX-2.bam -L tmpFolder/bedX.bed \n")
	Seq("java",MemString,"-jar",GATKJar,"-T","IndelRealigner","-R",refFile,"-I",regBam, "-targetIntervals",regInt,"-o",reg2Bam,"-L",bedBed)!

	// Delete tmpFolder/regionX.bam, tmpFolder/regionX.bai, tmpFolder/regionX.intervals
	Files.deleteIfExists(Paths.get(regBam))
	Files.deleteIfExists(Paths.get(tmpFolder + "region" + chrRegion + ".bai"))
	Files.deleteIfExists(Paths.get(regInt))

	
	//==========================================Base quality recalibration========================
	val regTab = tmpFolder + "region" + chrRegion + ".table"
	logvari.write(getTimeStamp()+": java MemString -jar toolsFolder/GenomeAnalysisTK.jar -T BaseRecalibrator -nct numOfThreads -R refFolder/RefFileName -I tmpFolder/regionX-2.bam -o tmpFolder/regionX.table -L tmpFolder/bedX.bed --disable_auto_index_creation_and_locking_when_reading_rods \n")
	Seq("java",MemString,"-jar",GATKJar,"-T","BaseRecalibrator","-nct",numOfThreads,"-R",refFile,"-I",reg2Bam,"-o",regTab,"-L",bedBed,
		"--disable_auto_index_creation_and_locking_when_reading_rods","-knownSites",refFolder + SnpFileName)!
 
	val reg3Bam = tmpFolder + "region" + chrRegion + "-3.bam"
	logvari.write(getTimeStamp()+": java MemString -jar toolsFolder/GenomeAnalysisTK.jar -T PrintReads -R refFolder/RefFileName -I tmpFolder/regionX-2.bam -o tmpFolder/regionX-3.bam -BQSR tmpFolder/regionX.table -L tmpFolder/bedX.bed \n")
	Seq("java",MemString,"-jar",GATKJar,"-T","PrintReads","-R",refFile, "-I",reg2Bam,"-o",reg3Bam,"-BQSR",regTab,"-L",bedBed)!

	// Delete tmpFolder/regionX-2.bam, tmpFolder/regionX-2.bai, tmpFolder/regionX.table
	Files.deleteIfExists(Paths.get(reg2Bam))
	Files.deleteIfExists(Paths.get(tmpFolder + "region" + chrRegion + "-2.bai"))
	Files.deleteIfExists(Paths.get(regTab))	


	//================================================Haplotype -> Uses the region bed file==========
	val regionVcf = tmpFolder + "region" + chrRegion + ".vcf"
	logvari.write(getTimeStamp()+": java MemString -jar toolsFolder/GenomeAnalysisTK.jar -T HaplotypeCaller -nct numOfThreads -R refFolder/RefFileName -I tmpFolder/regionX-3.bam -o tmpFolder/regionX.vcf  -stand_call_conf 30.0 -stand_emit_conf 30.0 -L tmpFolder/bedX.bed \n")
	Seq("java",MemString,"-jar",GATKJar,"-T","HaplotypeCaller","-nct",numOfThreads,"-R",refFile,"-I",reg3Bam,"-o",regionVcf,"-stand_call_conf","30.0",
		"-stand_emit_conf","30.0","-L",bedBed,"--no_cmdline_in_header","--disable_auto_index_creation_and_locking_when_reading_rods")!

	// Delete tmpFolder/regionX-3.bam, tmpFolder/regionX-3.bai, tmpFolder/bedX.bed
	Files.deleteIfExists(Paths.get(reg3Bam))
	Files.deleteIfExists(Paths.get(tmpFolder + "region" + chrRegion + "-3.bai"))
	Files.deleteIfExists(Paths.get(bedBed)) 
	//==================================================VCF file processing==========================
	// Return the content of the vcf file produced by the haplotype caller in the form of <Chromsome number, <Chromosome Position, line>>
    val vcffilter = Source.fromFile(regionVcf).getLines.filter(_.startsWith("chr"))
	logvari.write(getTimeStamp()+": Cutting the vcf header \n")
    val vcfrec = new ArrayBuffer[(Integer, (Integer, String))]()
    for (line <- vcffilter) {
      val linevcf = line.split("\t")
      val chrNum = linevcf(0).substring(3) match {
            case "X" => 23
            case "Y" => 24
            case _ => linevcf(0).substring(3).toInt
          }
     
      val pos = linevcf(1).toInt 
	  //get position
      vcfrec += new Pair(chrNum,new Pair(pos,line))
	  //make a record and append to Arraybuffer
    }
	Files.deleteIfExists(Paths.get(regionVcf)) 
	Files.deleteIfExists(Paths.get(regionVcf+".idx"))
	//delete temp files 
	logvari.close()
	
	return vcfrec.toArray

	}

def loadbalancer(total:Int, totalreg:Int): Array[Int] = {
	val balanced = new Array[Int](total)
	//this is array of int of size=total number of all records combined 
	//this will have value of region index
	val Inumber = total/totalreg
	//ideal number for equal distribution of the load  
	val InumberADD = Inumber + 1
	//ideal number +1 
	val Offset = total%totalreg
	//remaining left over records number 
	
	var Rindex :Int= 0
	//declararion of variable
	
	while (Rindex < totalreg){
	//iterate till Rindex < number of region 
	//value of Rindex will be from 0 to 3 
		if (Rindex < Offset){
			//if Rindex is less than offset 
			for (i <- Rindex*InumberADD to ((Rindex+1)*InumberADD - 1)){
				
				balanced(i) = Rindex
			}
			//this for loop will add 0 to balanced[0] to balanced [2777301] means 
			//records in these index belong to region 0 
			//Further, this will iterate till region 2 
		}
		else{
			for (i <- (Offset*InumberADD + (Rindex-Offset)*Inumber) to (Offset*InumberADD + (Rindex-Offset+1)*Inumber - 1)){
				balanced(i) = Rindex
			}
			//for region 3, remaining indeces are belong to region 3  
		}
		Rindex = Rindex + 1 
		//iterate for next region 
	}

	return balanced
}




def compareSAM(input1:SAMRecord ,input2:SAMRecord): Int = {
		//function to compare SAM records for sorting
        val chrnum1 = input1.getReferenceIndex().toInt;
        val chrnum2 = input2.getReferenceIndex().toInt;
		//get chromosome number of input SAM records 
        if (chrnum1 == -1) {
		//check if first SAM chr num is -1
			if (chrnum2 == -1){ return 0;} else{ return 1; }
            //return 0 means both has same chr num -1
			//else return 1 means record 2 has bigger chr num so sort them
        } else if (chrnum2 == -1) {
		//if chrnu1 is non negative 
		
            return -1;
			//but chr2 is negative return -1  means keep records as it is 
        }
        val cmp = (chrnum1 - chrnum2).toInt;
		//if both chr num are positive , take difference 
        if (cmp != 0) {
		//diff is not zero means chr num are different
            return cmp;
		//return diff , this will sort them accordingly	
        }
        return (input1.getAlignmentStart() - input2.getAlignmentStart()).toInt;
		//if both chr num are positive and same 
		//compare wtr their position given by getAlignmentStart
    }
	
	
def main(args: Array[String]) 
{
	val config = new Configuration()
	config.initialize()
	val numInstances = config.getNumInstances.toInt
	val inputFolder = config.getInputFolder
	val tmpFolder = config.getTmpFolder
		 
	val conf = new SparkConf().setAppName("DNASeqAnalyzer")
	// For local mode, include the following two lines
	conf.setMaster("local[" + config.getNumInstances() + "]")
	conf.set("spark.cores.max", config.getNumInstances())
	
	val sc = new SparkContext(conf)
	
	// Comment these two lines if you want to see more verbose messages from Spark
	Logger.getLogger("org").setLevel(Level.OFF);
	Logger.getLogger("akka").setLevel(Level.OFF);
	
	val configBroad = sc.broadcast(config)
	//broadcasting the configurationfile		
	var t0 = System.currentTimeMillis
	
	// Rest of the code goes here
	
	
	val inputDir = new File(inputFolder) 
	//get input chunk directory


	val rawfiles = inputDir.listFiles.filter(_.isFile).map(_.getName) 
	//read list of raw files 
	//filter with .isfile to get files from the directory
	//map to the name of the files  
	println("Input files====================")
	rawfiles.foreach(println)
	println("===============================")

	val chunk = sc.parallelize(rawfiles,numInstances)
	//parallelize the input chunks according to numInstances

	//==============================================bwa call =======================================================	
	val SAMRDD = chunk.mapPartitions{partitionIterator=>partitionIterator.map(x=>bwaRun(x,configBroad.value))}
	//use mappartitions for each partition to run bwa in parallel 
	// bwa (a,config) a== input file chunk
	//				  b==config file

	//==============================================load balancing==================================================
	val SAMRDDIndex = SAMRDD.flatMap{a=>a}.sortByKey().zipWithIndex.map{case(a,b)=>(b,a)}
	//map and flat the key value pairs after that sort it by chromosome number 
	//further map to get unique index for each SAM record of format (index,(chrnum,SAM record))
	//example:::
	//(0,(1,11V6WR1:111:D1375ACXX:1:1101:8891:93807 1/2 100b aligned read.))
	//(1,(1,11V6WR1:111:D1375ACXX:1:1101:6561:5142 2/2 100b aligned read.))
	//(2,(1,11V6WR1:111:D1375ACXX:1:1101:8891:93807 2/2 100b aligned read.))
	//(3,(1,11V6WR1:111:D1375ACXX:1:1101:6710:5172 1/2 100b aligned read.))

	val Totalnum = SAMRDDIndex.count.toInt
	//Find total number of records for loadbalancing

	val Balancer = loadbalancer(Totalnum,numInstances)
	//call load balancer

	// <chromosome region, SAM Record>
	val regionRDD = SAMRDDIndex.partitionBy(new RangePartitioner(numInstances,SAMRDDIndex)).mapPartitions{partitionIterator=>partitionIterator.map(x=>(Balancer(x._1.toInt),x._2._2))}	
	//partitions after the loadbalancing of type (chroregionnum,(chrnum,record))
	// example 
	//(0,11V6WR1:111:D1375ACXX:1:1101:8891:93807 1/2 100b aligned read.)
	//(0,11V6WR1:111:D1375ACXX:1:1101:20562:60471 1/2 100b aligned read.)
	//(0,11V6WR1:111:D1375ACXX:1:1101:8891:93807 2/2 100b aligned read.)
	//(0,11V6WR1:111:D1375ACXX:1:1101:20562:60471 2/2 100b aligned read.)

	//=============================================== parallel variant calling========================================	
	val variRDD = regionRDD.groupByKey.mapPartitions{partitionIterator=>partitionIterator.flatMap(a=>variantCall(a._1,a._2.toArray.sortWith(compareSAM(_,_)<0),configBroad.value))}	   
	//first step is to groupByKey the records according to region number. There will be total 4 groups
	//then use mapPartitions to run partitions in parallel
	//call the variantcall method but we need to sort the record before that using compareSAM method
	//compareSAM method will sort SAM records by position 	


	
	//============================================== Sorting=========================================================
	val variRDDSort = variRDD.groupByKey.mapValues{v=>v.toArray.sortWith(_._1 < _._1)}.sortByKey()

	//============================= writing final output file===================================================
	val vcfLines = variRDDSort.flatMapValues(v=>v).map(x => x._2._2).collect
	val outputFolder = config.getOutputFolder
	val bw = new FileWriter(new File(outputFolder + "final.vcf"))
	bw.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tls\n")
	for (l <- vcfLines){
		bw.write(l + "\n")
	}
	bw.close()

	val et = (System.currentTimeMillis - t0) / 1000 
	println("|Execution time: %d mins %d secs|".format(et/60, et%60))
}
//////////////////////////////////////////////////////////////////////////////
} // End of Class definition
