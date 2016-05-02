package com.ibm.bpm

import java.io.ByteArrayInputStream
import java.util.HashMap
import scala.collection.JavaConversions.asScalaBuffer
import org.apache.commons.codec.binary.Base64
import java.io.FileOutputStream
import java.io.BufferedOutputStream
import com.ibm.watson.developer_cloud.visual_recognition.v2.VisualRecognition
import com.ibm.watson.developer_cloud.alchemy.v1.AlchemyLanguage
import scala.collection.immutable.ListMap

class CallWatson {
  val watson = new VisualRecognition("2015-12-02")

  def classifyImage(username: String, password: String, fileName: String, b64ImageString: String): HashMap[String, java.lang.Double] = {
    val responseMap = new HashMap[String, java.lang.Double]
    watson.setUsernameAndPassword(username, password)
    println("BU: converting image")
    val imageBytes = b64ImageString.getBytes
    val image = Base64.decodeBase64(imageBytes)
    val imageStream = new ByteArrayInputStream(image)
    println("BU: uploading to Watson for classification")
    val recognizedImage = watson.classify(fileName, imageStream, null)
    println("BU: classification complete, getting scores")
    recognizedImage.getImages.foreach { x =>
      x.getScores.foreach { y =>
        responseMap.put(y.getName, y.getScore)
      }
    }
    recognizedImage.getImages.foreach { x => println("BU: " + x) }

    responseMap
  }

  def getPropertyTaxonomy(apiKey: String, properties: String): String = {
    val params = new HashMap[String, Object]
    val alchemy = new AlchemyLanguage
    alchemy.setApiKey(apiKey)
    params.put(AlchemyLanguage.TEXT, properties)
    println("BU: getting taxonomy for " + params.toString())
    val taxonomies = alchemy.getTaxonomy(params)
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

}