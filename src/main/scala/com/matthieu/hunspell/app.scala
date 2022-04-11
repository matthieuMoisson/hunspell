package com.matthieu.hunspell

import com.atlascopco.hunspell.Hunspell
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`

object app {

  def getWord(sentence: String) : String = {
    sentence.split(" ")(1).split(":")(1)
  }

  def main (args: Array[String]): Unit = {
    if(args.size != 3)
      throw new Exception("Need 3 args, dic, aff, and file")
    val pathDic: String = args(0)
    val pathAff: String = args(1)
    val pathFile: String = args(2)

    lazy val spark: SparkSession = SparkSession
      .builder
      .appName(s"Hunspell")
      .config("spark.master", "local")
      .getOrCreate

    val words: DataFrame = spark.read.text(pathFile)
    import spark.implicits._
    words.show()
    val lemmas: Dataset[String] = words.map(word => {
      val speller = new Hunspell(pathDic, pathAff)
      speller.analyze(word.getString(0))
        .toList
        .map(lemma => getWord(lemma))
        .distinct
        .mkString("+")
    })

    lemmas.show(false)
    lemmas.write.mode(SaveMode.Overwrite).csv("src/main/ressources/output.csv")
  }
}
