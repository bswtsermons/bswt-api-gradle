package org.bswt.bag

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.S3ObjectSummary
import groovy.util.logging.Slf4j
import java.nio.file.Paths
import org.hidetake.groovy.ssh.Ssh

import java.util.concurrent.TimeUnit

import com.amazonaws.regions.Regions

@Slf4j
class WavRepositoryService {
	def plinkPath = Paths.get(System.properties.'bswt.wrp.plinkPath')
	def pscpPath = Paths.get(System.properties.'bswt.wrp.pscpPath')
	def lamePath = Paths.get(System.properties.'bswt.wrp.lamePath')
	def localWavDir = Paths.get(System.properties.'bswt.wrp.dir.wav')
	def localMp3Dir = Paths.get(System.properties.'bswt.wrp.dir.mp3')
	def localHqMp3Dir = Paths.get(System.properties.'bswt.wrp.dir.mp3.hq')
	
	def bucketName = System.properties.'bswt.wrp.aws.s3.audio.bucket'
	def sshUser = System.properties.'bswt.wrp.ssh.user'
	def sshHost = System.properties.'bswt.wrp.ssh.host'
	def bswtMp3Dir = System.properties.'bswt.wrp.ssh.dir.mp3'
	def bswtHqMp3Dir = System.properties.'bswt.wrp.ssh.dir.mp3.hq'
	
	def ssh = Ssh.newService()
	
	def checkConfigs()	{
		assert plinkPath.toFile().exists() : "Specified plink executable exists"
		assert pscpPath.toFile().exists() : "Specified pscp executable exists"
		assert lamePath.toFile().exists() : "Specified lame executable exists"
		
		// maybe we want this to create automatically instead?
		assert localWavDir.toFile().exists() : "Specified wav directory does not exist"
		assert localMp3Dir : "No local mp3 directory specified"
		assert localHqMp3Dir : "No local hq mp3 directory specified"
		
		assert bucketName : "No aws s3 audio bucket name specified"
		assert sshUser : "No ssh user specified"
		assert sshHost : "No ssh host specified"
		assert bswtMp3Dir : "No remote mp3 directory specified"
		assert bswtHqMp3Dir : "No remote hq mp3 directory specified"
	}
	
	def executeCommand(cmd) {
		log.debug 'executing command {}', cmd
		def process = new ProcessBuilder(cmd)
				.redirectErrorStream(true)
				.start()
		process.waitFor()
		if (process.exitValue() != 0) {
			log.error "could not execute command, error follows"
			process.inputStream.eachLine { log.error it }
		}
		assert process.exitValue() == 0
		
		process
	}
	
	def init() {
		ssh.settings {
			knownHosts = allowAnyHosts
		}
		ssh.remotes {
			bswt {
				host = sshHost
				user = sshUser
				agent = true
			}
		}
	}
	
	def executePlinkCommand(cmd) {
		executeCommand([ plinkPath.toString(), "-batch", sshUser+"@"+sshHost ] + cmd)
	}
	
	def pscpCopy(file1, file2) {
		executeCommand( [ pscpPath.toString(), "-q", file1, file2 ])
	}
	
	def getAmazonS3Wavs() {
		def s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build()
		
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
		def sids = []
		
		ssh.run {
			session(ssh.remotes.bswt) {
				execute("ls ${bswtMp3Dir}/*.mp3").eachLine {
					def match = it =~ /(\d{10})\.mp3$/;
					if (match) {
						sids.add match[0][1]
					}
						
				}
			}
		}
		
		log.debug 'bswt sids: {}', sids
		sids

	}
	
	def getLocalWavs() {
		if (localWavDir.toFile().exists()) {
			log.info 'wav direcotry {} did not exist; creating', localWavDir
		}
		def sids = localWavDir.toFile().list().findAll { it =~ /^\d{10}\.wav$/}
		                                      .collect { it.substring(0, it.lastIndexOf(".")) }
		
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
		ssh.run {
			session(ssh.remotes.bswt) {
				put from: localPath, into: remotePath+'.part'
				log.debug 'rename partial'
				execute("mv ${remotePath}.part ${remotePath}") 
			}
		}
		
		log.debug 'uploading hq mp3'
		ssh.run {
			session(ssh.remotes.bswt) {
				put from: localHqPath, into: remoteHqPath+'.part'
				log.debug 'rename partial'
				execute("mv ${remoteHqPath}.part ${remoteHqPath}") 
			}
		}
		
	}
	
	def makeMp3s(String sid) {
		if (!localHqMp3Dir.toFile().exists()) {
			log.info 'mp3 directory {} did not exist; creating', localHqMp3Dir
			localHqMp3Dir.toFile().mkdirs();
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
		wrp.checkConfigs()
		wrp.init()
		
		def existingSids = wrp.amazonS3Wavs + wrp.BSWTWavs
		wrp.localWavs.findAll { !(it in existingSids) }.each {
			wrp.makeMp3s it
			wrp.uploadLocalWav it
		}
		
	}
}

