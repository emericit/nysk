/* Analyse des articles de presse sur l'afaire DSK */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd

import scala.xml._

import org.apache.hadoop.io.{ Text, LongWritable }
import org.apache.hadoop.conf.Configuration

import com.cloudera.datascience.common.XmlInputFormat

import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}

import java.sql.Timestamp
import java.text.SimpleDateFormat

object w2vec {

  // Séparation du fichier XML en un RDD où chaque élément est un article
  // Retourne un RDD de String à partir du fichier "path"
  def loadArticle(sc: SparkContext, path: String): RDD[String] = {
    @transient val conf = new Configuration()
    conf.set(XmlInputFormat.START_TAG_KEY, "<document>")
    conf.set(XmlInputFormat.END_TAG_KEY, "</document>")
    val in = sc.newAPIHadoopFile(path, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text], conf)
    in.map(line => line._2.toString)
  }

  // Pour un élément XML de type "document",
  //   - on extrait le champ "date"
  //   - on parse la chaîne de caractère au format yyyy-MM-dd HH:mm:ss
  //   - on retourne un Timestamp
  def extractDate(elem: scala.xml.Elem): java.sql.Timestamp = {
    val dn: scala.xml.NodeSeq = elem \\ "date"
    val x: String = dn.text
    // d'après l'exemple 2011-05-18 16:30:35
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    if (x == "")
      return null
    else {
      val d = format.parse(x.toString());
      val t = new Timestamp(d.getTime());
      return t
    }
  }

  // Pour un élément XML de type "document",
  //   - on extrait le champ #field
  def extractString(elem: scala.xml.Elem, field: String): String = {
    val dn: scala.xml.NodeSeq = elem \\ field
    val x: String = dn.text
    return x
  }

  def extractInt(elem: scala.xml.Elem, field: String): Int = {
    val dn: scala.xml.NodeSeq = elem \\ field
    val x: Int = dn.text.toInt
    return x
  }

  def extractAll(elem: scala.xml.Elem, whatText: String = "text"): (Int, java.sql.Timestamp, String) = {
    return (extractInt(elem,"docid"), extractDate(elem), extractString(elem,whatText))
  }

 def extractText(elem: scala.xml.Elem): String = {
    return (extractString(elem,"title") + " " + extractString(elem,"summary") + " " + extractString(elem,"text"))
  }


  // Nécessaire, car le type java.sql.Timestamp n'est pas ordonné par défaut (étonnant...)
  implicit def ordered: Ordering[java.sql.Timestamp] = new Ordering[java.sql.Timestamp] {
    def compare(x: java.sql.Timestamp, y: java.sql.Timestamp): Int = x compareTo y
  }

  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("fitw2vec")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://head.local:9000"), hadoopConf)
    val nysk_raw = loadArticle(sc, "hdfs://head.local:9000/user/emeric/nysk.xml")/*.sample(false,0.01)*/
    val nysk_xml: RDD[Elem] = nysk_raw.map(XML.loadString)
    val nysk: RDD[Seq[String]] = nysk_xml.map(e => extractText(e).toLowerCase.split("\\W+").toSeq)

    val input = sc.textFile("text8").map(line => line.split(" ").toSeq)

    val word2vec = new Word2Vec()
    val w2vModel = word2vec.fit(input.union(nysk))    
    val outputModel = "hdfs://head.local:9000/user/emeric/w2vModel2"
        try { hdfs.delete(new org.apache.hadoop.fs.Path(outputModel), true) } 
        catch { case _ : Throwable => { } }
    w2vModel.save(sc, outputModel)
    sc.stop()
  }
}
