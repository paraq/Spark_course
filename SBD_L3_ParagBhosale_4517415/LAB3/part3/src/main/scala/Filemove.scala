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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

class Filemove(local: String,server: String)
{
val hadoopConf = new Configuration()
val hdfs = FileSystem.get(hadoopConf)
val localPath = new Path(local)
val clusterPath = new Path(server)
	
	def upload() 
	{
		hdfs.copyFromLocalFile(localPath, clusterPath) // command will put local file to the hdfs directory
		println("Complete uploading file"+localPath+" to the cluster:"+clusterPath)
	}
	
	def download() 
	{
		hdfs.copyToLocalFile(clusterPath, localPath) // command will get file from hdfs to the local directory
		println("Complete downloading file"+clusterPath+" to the local:"+localPath)
	}
	
   
}
