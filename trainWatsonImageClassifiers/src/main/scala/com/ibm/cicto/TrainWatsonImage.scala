package com.ibm.cicto

import java.io.File
import java.util.ArrayList

import com.ibm.watson.developer_cloud.visual_recognition.v3.VisualRecognition
import com.ibm.watson.developer_cloud.visual_recognition.v3.model.ClassifyImagesOptions
import com.ibm.watson.developer_cloud.visual_recognition.v3.model.CreateClassifierOptions
import com.ibm.watson.developer_cloud.visual_recognition.v3.model.VisualClassifier.Status

object TrainWatsonImage extends App {

  override def main(args: Array[String]) {
    val watson = new VisualRecognition(VisualRecognition.VERSION_DATE_2016_05_19)
    //TODO replace with args(1)
    watson.setApiKey("90536110df2adddf55f9c050157908d7d01cb3a7")

    var classifierExists = false
    var classifierName = ""
    val cogClassifier = watson.getClassifiers.execute()
    import scala.collection.JavaConversions._
    cogClassifier.foreach(x => {
      if (x.getName.contains("cognitiveClaims")) {
        classifierExists = true
        classifierName = x.getId
        println("Classifier " + classifierName + " in state " + x.getStatus + " already exists.")
        //Uncomment next line if you want to build a new classifier anyway
        //watson.deleteClassifier(classifierName).execute()
      }
    });

    if (!classifierExists) {
      println("Creating ClassifierBuilder")
      val createOptions = new CreateClassifierOptions.Builder()
        .classifierName("cognitiveClaims")
        .addClass("car", new File("src/test/resources/cars.zip"))
        .addClass("trucks", new File("src/test/resources/trucks.zip"))
        .negativeExamples(new File("src/test/resources/tractors.zip"))
        .build()
      println("Creating classifier in Watson")
      val cognitiveClaims = watson.createClassifier(createOptions).execute()
      println(cognitiveClaims)
    }

    var classifierReady = false
    while (!classifierReady) {
      val cogClassifier = watson.getClassifiers.execute()
      cogClassifier.foreach(x => {
        if (x.getName.contains("cognitiveClaims")) {
          classifierName = x.getId
          println("Classifier " + classifierName + " in state " + x.getStatus + ".")
          if (x.getStatus.equals(Status.AVAILABLE)) {
            classifierReady = true
          }
          Thread.sleep(5000)
        }
      });
    }

    println("Classifying an image of a car")
    var classifierIds = new ArrayList[String](1)
    classifierIds.add(0, "car")
    val carOptions = new ClassifyImagesOptions.Builder()
      .images(new File("src/test/resources/audi_PNG1736.jpg"))
      .classifierIds(classifierIds)
      .build();
    println(watson.classify(carOptions).execute())

    println("Classifying an image of a truck")
    classifierIds.add(1, "truck")
    val truckOptions = new ClassifyImagesOptions.Builder()
      .images(new File("src/test/resources/17.jpg"))
      //.classifierIds(classifierIds)
      .build();
    println(watson.classify(truckOptions).execute())

    println("Classifying an image of a tractor")
    val tractorOptions = new ClassifyImagesOptions.Builder()
      .images(new File("src/test/resources/tractor.jpg"))
      .classifierIds(classifierIds)
      .build();
    println(watson.classify(tractorOptions).execute())

  }

}