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

class CallWatson {
  val watson = new VisualRecognition(VisualRecognition.VERSION_DATE_2016_05_19)
  val PREFIX = "stream2file"

  val SUFFIX = ".png" //must be either png or jpg for Watson image service to work.

  def classifyImage(apiKey: String, username: String, password: String, fileName: String, b64ImageString: String): String = {
    if (!Option(apiKey).getOrElse("").isEmpty()) {
      watson.setApiKey(apiKey)
      watson.setEndPoint("http://rtclaussproxy.mybluemix.net/visual-recognition/api")
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
    //Add default classifiers to work around a bug in 3.0.1 of Watson Client
    val classifiers = new ArrayList[String]()
    classifiers.add("default")
    val options = new ClassifyImagesOptions.Builder()
      .images(tmpFile)
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
    if (taxonomies.getTaxonomy.size() > 0) {
      taxonomies.getTaxonomy.foreach { x =>
        if (x.getConfident) {
          label = x.getLabel()
          confident = true
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