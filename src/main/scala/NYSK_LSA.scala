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
import com.cloudera.datascience.lsa.ParseWikipedia._
import com.cloudera.datascience.lsa.RunLSA._

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import breeze.linalg.{DenseMatrix => BDenseMatrix, DenseVector => BDenseVector, SparseVector => BSparseVector}
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors, DenseVector, SparseVector}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.util.KMeansDataGenerator

import java.sql.Timestamp
import java.text.SimpleDateFormat

object NYSK_LSA {
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
  //   - on extrait le champ "text"
  def extractText(elem: scala.xml.Elem): String = {
    val dn: scala.xml.NodeSeq = elem \\ "text"
    val x: String = dn.text
    return x
  }

  // Pour un élément XML de type "document",
  //   - on extrait le champ "summary"
  def extractSummary(elem: scala.xml.Elem): String = {
    val dn: scala.xml.NodeSeq = elem \\ "summary"
    val x: String = dn.text
    return x
  }

  // Pour un élément XML de type "document",
  //   - on extrait le champ "title"
  def extractTitle(elem: scala.xml.Elem): String = {
    val dn: scala.xml.NodeSeq = elem \\ "title"
    val x: String = dn.text
    return x
  }

  // Pour un élément XML de type "document",
  //   - on extrait le champ "source"
  def extractSource(elem: scala.xml.Elem): String = {
    val dn: scala.xml.NodeSeq = elem \\ "source"
    val x: String = dn.text
    return x
  }

  def extractAll(elem: scala.xml.Elem): (java.sql.Timestamp, String, String, String, String) = {
    return (extractDate(elem), extractSource(elem), extractTitle(elem), extractSummary(elem), extractText(elem))
  }

  // Nécessaire, car le type java.sql.Timestamp n'est pas ordonné par défaut (étonnant...)
  implicit def ordered: Ordering[java.sql.Timestamp] = new Ordering[java.sql.Timestamp] {
    def compare(x: java.sql.Timestamp, y: java.sql.Timestamp): Int = x compareTo y
  }

  def main_(args: Array[String]) {
    val conf = new SparkConf().setAppName("NYSK")
    val sc = new SparkContext(conf)
    val nysk_raw = loadArticle(sc, "/user/emeric/nysk.xml")
    val nysk_xml: RDD[Elem] = nysk_raw.map(XML.loadString)
    //val nysk_timestamps: RDD[java.sql.Timestamp] = nysk_xml.map(extractDate)
    /*println(nysk_raw.count() + " articles covering " + nysk_timestamps.min() + " to " + nysk_timestamps.max())*/
    //val nysk_texts: RDD[String] = nysk_xml.map(extractText)
    /*println(nysk_texts.count())*/
    val nysk: RDD[(java.sql.Timestamp, String, String, String, String)] = nysk_xml.map(extractAll)

    val stopwords = sc.textFile("/user/emeric/stopwords.txt").collect.toArray.toSet
    val stopwordsBroadcast = sc.broadcast(stopwords).value

    // mapPartitions pour ne pas initialiser le NLPPIpeline 1 fois par élément du RDD
    // mais une fois par noeud de calcul
    val lemmatized = nysk.mapPartitions(iter => {
      val pipeline = com.cloudera.datascience.lsa.ParseWikipedia.createNLPPipeline();
      iter.map {
        case (date, source, title, summary, text) =>
          (title,
            com.cloudera.datascience.lsa.ParseWikipedia.plainTextToLemmas(title + " " + summary + " " + text, stopwordsBroadcast, pipeline))
      };
    })

    val numTerms = 1000;
    val (termDocMatrix, termIds, docIds, idfs) = com.cloudera.datascience.lsa.ParseWikipedia.termDocumentMatrix(lemmatized, stopwordsBroadcast, numTerms, sc);
    termDocMatrix.cache()

    val mat = new RowMatrix(termDocMatrix)
    val k = 200 // nombre de valeurs singuliers à garder
    val svd = mat.computeSVD(k, computeU = true)

    val topConceptTerms = com.cloudera.datascience.lsa.RunLSA.topTermsInTopConcepts(svd, 10, 10, termIds)
    val topConceptDocs = com.cloudera.datascience.lsa.RunLSA.topDocsInTopConcepts(svd, 10, 10, docIds)
    for ((terms, docs) <- topConceptTerms.zip(topConceptDocs)) {
          println("Concept terms: " + terms.map(_._1).mkString(", "));
          println("Concept docs: " + docs.map(_._1).mkString(", "));
          println();
       }

    sc.stop()
  }
}
