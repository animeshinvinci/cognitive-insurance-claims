package com.ibm.bpm

import java.io.ByteArrayInputStream
import java.io.File
import java.io.FileOutputStream
import java.io.InputStream
import java.util.ArrayList
import java.util.HashMap

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.immutable.ListMap

import org.apache.commons.codec.binary.Base64
import org.apache.commons.io.IOUtils

import com.ibm.watson.developer_cloud.alchemy.v1.AlchemyLanguage
import com.ibm.watson.developer_cloud.util.GsonSingleton
import com.ibm.watson.developer_cloud.visual_recognition.v3.VisualRecognition
import com.ibm.watson.developer_cloud.visual_recognition.v3.model.ClassifyImagesOptions
import com.lambdaworks.redis.RedisClient
import scala.util.Try
import java.io.FileInputStream
import com.lambdaworks.redis.RedisException

class CallWatson {
  val watson = new VisualRecognition(VisualRecognition.VERSION_DATE_2016_05_20)
  val PREFIX = "stream2file"

  val SUFFIX = ".png" //must be either png or jpg for Watson image service to work.

  def classifyImage(apiKey: String, username: String, password: String, fileName: String, b64ImageString: String): String = {
    if (!Option(apiKey).getOrElse("").isEmpty()) {
      watson.setApiKey(apiKey)
      //watson.setEndPoint("http://rtclaussproxy.mybluemix.net/visual-recognition/api")
    } else {
      watson.setUsernameAndPassword(username, password)
    }
    println("BU: converting image")
    val imageBytes = b64ImageString.getBytes
    val image = Base64.decodeBase64(imageBytes)
    val imageStream = new ByteArrayInputStream(image)

    println("BU: Saving image to temp file.")
    val tmpFile = stream2file(imageStream)
    println("BU: received file.  building up options")

    //rtclauss 9/19/2016 - Add Redis Caching
    checkRedisCache(tmpFile)
  }

  def callWatsonImageRecognition(file: File): String = {
    //Add default classifiers to work around a bug in 3.0.1 of Watson Client
    val classifiers = new ArrayList[String]()
    classifiers.add("default")
    val options = new ClassifyImagesOptions.Builder()
      .images(file)
      .classifierIds(classifiers)
      .build()
    println("BU: done building. calling watson")
    val recognizedImage = watson.classify(options).execute()
    println("BU: classification complete, getting scores")
    var response = ""
    recognizedImage.getImages.foreach { x =>
      x.getClassifiers.foreach { y =>
        response += GsonSingleton.getGson().toJson(y.getClasses)
        println("BU here is the response " + response)
      }
    }
    response = response.toString().replace("class", "className")
    println(response)
    response
  }

  def getPropertyTaxonomy(apiKey: String, properties: String): String = {
    val params = new HashMap[String, Object]
    val alchemy = new AlchemyLanguage
    alchemy.setApiKey(apiKey)
    params.put(AlchemyLanguage.TEXT, properties)
    println("BU: getting taxonomy for " + params.toString())
    val taxonomies = alchemy.getTaxonomy(params).execute()
    println("BU: taxonomies retrieved " + taxonomies)
    var label = ""
    var confident = false
    var confidenceScore = 0.0
    if (taxonomies.getTaxonomy.size() > 0) {
      taxonomies.getTaxonomy.foreach { x =>
        if (x.getConfident && x.getScore > confidenceScore) {
          label = x.getLabel()
          confidenceScore = x.getScore
          confident = true
          println("BU: we're confident " + x.getLabel + " is the label with score of " + x.getScore)
        }
      }
      if (!confident) {
        val items = taxonomies.getTaxonomy.flatMap(_.getLabel.split("/")).toList.groupBy((word: String) => word).mapValues(_.length)
        val sortedItems = ListMap(items.toSeq.sortWith(_._2 > _._2): _*).toSeq
        println("BU: sorted -> " + sortedItems)
        label = sortedItems(0)._1
      }
    } else {
      label = "other"
    }
    println("BU: determined label -> " + label)
    label
  }

  def checkRedisCache(file: File): String = {
    println("entered checkRedisCache")
    var recognition = ""
    println("BU: connecting to redis")
    try {
      val redisClient = RedisClient.create("redis://x:DQJIQGKUYGBCNKTN@sl-us-dal-9-portal.1.dblayer.com:15146")
      println("BU: redis client created, connecting")
      val redisConnection = redisClient.connect
      println("BU: connection to Redis on Compose successful.  getting cached value")
      val fis = new FileInputStream(file)
      val fileSha1 = org.apache.commons.codec.digest.DigestUtils.sha1Hex(fis)
      fis.close()

      val cachedValue = redisConnection.get(fileSha1)
      println("BU: cachedValue " + cachedValue)
      if (cachedValue == null) {
        println("BU: image not cached in redis, calling Watson")
        recognition = callWatsonImageRecognition(file)
        redisConnection.set(fileSha1, recognition)
        println("BU: result from Watson " + recognition)
      } else {
        recognition = Try(cachedValue.toString).getOrElse("")
      }
      println("shutting everything down and returning")
      redisConnection.close()
      redisClient.shutdown()
    } catch {
      case e: RedisException => {
        println("BU:  Error calling Redis by Compose.  Going direct to Watson.")
        recognition = callWatsonImageRecognition(file)
        println("BU: result from Watson" + recognition)
      }
    }
    return recognition
  }

  def stream2file(in: InputStream): File = {
    try {
      val tempFile = File.createTempFile(PREFIX, SUFFIX)
      tempFile.deleteOnExit()
      val out = new FileOutputStream(tempFile)
      IOUtils.copy(in, out)
      out.flush()
      IOUtils.closeQuietly(in)
      IOUtils.closeQuietly(out)
      println("BU: wrote image to " + tempFile)
      tempFile
    } catch {
      case e: Exception => {
        println("oops in writing image to file")
        println(e)
        null
      }
    }
  }
}