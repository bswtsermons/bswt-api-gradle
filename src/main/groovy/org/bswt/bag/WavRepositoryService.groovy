package org.bswt.bag

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.S3ObjectSummary
import groovy.util.logging.Slf4j
import java.nio.file.Paths

import java.util.concurrent.TimeUnit

import com.amazonaws.regions.Regions

@Slf4j
class WavRepositoryService {
	// TODO make this externally configurable
	def plinkPath = new File("c:\\Program Files (x86)\\PuTTY\\plink.exe")
	def pscpPath = new File("c:\\Program Files (x86)\\PuTTY\\pscp.exe")
	def lamePath = "C:\\Program Files\\lame3.100-64\\lame.exe"
	def sshUser = "bswt"
	def bucketName = 'audio.bswt.org'
	def sshHost = "www.bswt.org"
	def bswtMp3Dir = 'media/audio/mp3'
	def bswtHqMp3Dir = bswtMp3Dir + '/hq'
	def localWavDir = Paths.get System.properties.get('user.home'), 'Music', 'Services'
	def localMp3Dir = localWavDir.resolve 'mp3'
	def localHqMp3Dir = localMp3Dir.resolve 'hq'
	
	def executeCommand(cmd) {
		def process = new ProcessBuilder(cmd)
				.redirectErrorStream(true)
				.start()
		process.waitFor()
		if (process.exitValue() != 0) {
			println process.inputStream.eachLine { println it }
		}
		assert process.exitValue() == 0
		
		process
	}
	
	
	def executePlinkCommand(cmd) {
		executeCommand([ plinkPath.toString(), "-batch", sshUser+"@"+sshHost ] + cmd)
	}
	
	/*
	def uploadPartialThenRename(file1, file2) {
		log.debug 'uploading lq mp3'
		def process = pscpCopy([ file1, file2+'.part' ])
		log.debug 'rename partial {} to {}', file2+'.part', file2
		process = executePlinkCommand([ 'mv', file2+'.part', file2  ])
	}
	*/
	
	def pscpCopy(file1, file2) {
		executeCommand( [ pscpPath.toString(), "-q", file1, file2 ])
	}
	
	def getAmazonS3Wavs() {
		def s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build()
		//def ol = s3.listObjects('video.bswt.org');
		
		log.debug 'connecting to aws'
		boolean truncated = true
		def req = new ListObjectsV2Request().withBucketName(bucketName)
		def sids = []
		while (truncated) {
			def result = s3.listObjectsV2(req)
			
			sids += result.getObjectSummaries().findAll { it.key =~ /^\d{10}\.mp3$/ }
	                                           .collect { it.key.substring(0, it.key.lastIndexOf("."))	}
															  
			req.continuationToken = result.nextContinuationToken
			truncated = result.truncated
		}
		
		log.debug 'aws sids: {}', sids 
		
		sids
	}
	
	def getBSWTWavs() {
		def process = executePlinkCommand([ "cd", bswtMp3Dir+";", "ls", "*.mp3" ])
		def sids = process.inputStream.readLines().findAll() { it =~ /^\d{10}\.mp3$/ }.collect{
			def match = it =~ /^(\d{10})\.mp3$/
			match[0][1]
		}
		
		log.debug 'bswt sids: {}', sids
		
		sids
		
		/* get this to work with pageant
		def ssh = org.hidetake.groovy.ssh.Ssh.newService()
		ssh.settings {
			knownHosts = allowAnyHosts
			agentForwarding = true
		}
		ssh.remotes {
			bswt {
				host = 'www.bswt.org'
				user = 'bswt'
				agentForwarding = true
			}
		}
		
		ssh.run {
			session(ssh.remotes.bswt) {
				execute 'ls'
			}
		}
		*/
	}
	
	def getLocalWavs() {
		if (localWavDir.toFile().exists()) {
			log.info 'wav direcotry {} did not exist; creating', localWavDir
		}
		def sids = localWavDir.toFile().list().findAll { it =~ /^\d{10}\.wav$/}.collect { it.substring(0, it.lastIndexOf(".")) }
		
		log.debug 'local sids: {}', sids
		
		sids
	}
	
	def uploadLocalWav(String sid) {
		log.info 'uploading {}', sid 
		
		def localPath  = new File(localMp3Dir.toFile(), sid + '.mp3')
		def localHqPath = new File(localHqMp3Dir.toFile(), sid + '.mp3')
		def remotePath = bswtMp3Dir + "/" + sid+".mp3"
		def remoteHqPath = bswtHqMp3Dir + '/' + sid+'.mp3'
		
		log.debug 'uploading lq mp3'
		def process = pscpCopy([ localPath.toString(), sshUser+"@"+sshHost+":"+remotePath+'.part' ])
		log.debug 'rename partial'
		process = executePlinkCommand([ 'mv', remotePath+'.part', remotePath  ])
		
		log.debug 'uploading hq mp3'
		process = executeCommand([ pscpPath.toString(), "-q", localHqPath.toString(), sshUser+"@"+sshHost+":"+remoteHqPath+'.part' ])
		log.debug 'rename partial'
		process = executePlinkCommand([ 'mv', remoteHqPath+'.part', remoteHqPath  ])
		
		println remotePath
	}
	
	def makeMp3s(String sid) {
		if (!localHqMp3Dir.toFile().exists()) {
			log.info 'mp3 directory {} did not exist; creating', localHqMp3Dir
			localHqMp3Dir.mkdirs();
		}
		
		def wavFile = new File(localWavDir.toFile(), sid + '.wav')
		def mp3File = new File(localMp3Dir.toFile(), sid + '.mp3')
		def hqMp3File = new File(localHqMp3Dir.toFile(), sid + '.mp3')
		def mp3EncodingFile = new File(localHqMp3Dir.toFile(), sid + '.mp3.encoding')
		def hqMp3EncodingFile = new File(localHqMp3Dir.toFile(), sid + '.mp3.encoding')
		if (!mp3File.exists()) {
			log.info 'creating mp3 {}', mp3File
			log.debug 'using wav file {}', wavFile
			
			def process = executeCommand([lamePath.toString(), "-m", "m", "-V", "8", "--quiet", wavFile.toString(), mp3EncodingFile.toString()])
			
			hqMp3EncodingFile.renameTo mp3File
		}
		
		if (!hqMp3File.exists()) {
			log.info 'creating hq mp3 {}', hqMp3File
			log.debug 'using wav file {}', wavFile
			
			def process = executeCommand([lamePath.toString(), "-m", "m", "-V", "4", "--quiet", wavFile.toString(), hqMp3EncodingFile.toString()])
			
			hqMp3EncodingFile.renameTo hqMp3File
		}
		
	}
	
	static main(args) {
		def wrp = new WavRepositoryService()
		println wrp.localWavDir
		
		def existingSids = wrp.amazonS3Wavs + wrp.BSWTWavs
		println wrp.localWavs
		
		wrp.localWavs.findAll { !(it in existingSids) }.each {
			wrp.makeMp3s it
			wrp.uploadLocalWav it
		}
	}
}

