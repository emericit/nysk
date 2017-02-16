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
import breeze.linalg.{DenseMatrix => BDenseMatrix, DenseVector => BDenseVector, SparseVector => BSparseVector, Vector => BVector}
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}

import java.io.StringReader
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.process.CoreLabelTokenFactory;
import edu.stanford.nlp.process.DocumentPreprocessor;
import edu.stanford.nlp.process.PTBTokenizer;

import java.sql.Timestamp
import java.text.SimpleDateFormat

def toBreeze(v:Vector) = BVector(v.toArray)
def fromBreeze(bv:BVector[Double]) = Vectors.dense(bv.toArray)
def add(v1:Vector, v2:Vector) = fromBreeze(toBreeze(v1) + toBreeze(v2))
def scalarMultiply(a:Double, v:Vector) = fromBreeze(a * toBreeze(v))

object NYSK {
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
  def extract(elem: scala.xml.Elem, field: String): String = {
    val dn: scala.xml.NodeSeq = elem \\ field
    val x: String = dn.text
    return x
  }

  def extractAll(elem: scala.xml.Elem): (Int, java.sql.Timestamp, String) = {
    return (extract(elem,"docid"), extractDate(elem), extract(elem,"text"))
  }

  // Nécessaire, car le type java.sql.Timestamp n'est pas ordonné par défaut (étonnant...)
  implicit def ordered: Ordering[java.sql.Timestamp] = new Ordering[java.sql.Timestamp] {
    def compare(x: java.sql.Timestamp, y: java.sql.Timestamp): Int = x compareTo y
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("NYSK")
    val sc = new SparkContext(conf)
    val nysk_raw = loadArticle(sc, "/user/emeric/nysk.xml")
    val nysk_xml: RDD[Elem] = nysk_raw.map(XML.loadString)
    //val nysk_timestamps: RDD[java.sql.Timestamp] = nysk_xml.map(extractDate)
    /*println(nysk_raw.count() + " articles covering " + nysk_timestamps.min() + " to " + nysk_timestamps.max())*/
    //val nysk_texts: RDD[String] = nysk_xml.map(extractText)
    /*println(nysk_texts.count())*/
    val nysk: RDD[(Int, java.sql.Timestamp, String)] = nysk_xml.map(extractAll)

    val stopwords = sc.textFile("/user/emeric/stopwords.txt").collect.toArray.toSet
    val stopwordsBroadcast = sc.broadcast(stopwords).value

    val w2vModel = Word2VecModel.load(sc, "/user/emeric/w2vModel")
    // obtenir une Map[String, Array[Float]] sérializable
    //   mapValues seul ne retourne pas une map sérializable (SI-7005)
    val vectors = w2vModel.getVectors.mapValues(vv => Vectors.dense(vv.map(_.toDouble))).map(identity)
    // transmettre la map aux noeuds de calcul
    val bVectors = sc.broadcast(vectors)

    // mapPartitions pour ne pas initialiser le NLPPIpeline 1 fois par élément du RDD
    // mais une fois par noeud de calcul
    val lemmatized = nysk.mapPartitions(iter => {
      val pipeline = com.cloudera.datascience.lsa.ParseWikipedia.createNLPPipeline();
      iter.map {
        case (docid, date, text) =>
          (docid,
            com.cloudera.datascience.lsa.ParseWikipedia.plainTextToLemmas(text, stopwordsBroadcast, pipeline))
      };
    })

    var w2vecRepr = lemmatized.map({ case (docid, lemmas) => 
      var vSum = Vectors.zeros(100)
      var vNb = 0
      lemmas.foreach { word =>
           bVectors.value.get(word).foreach { v =>
                vSum = add(v, vSum)
                vNb += 1
           }
      }
      if (vNb != 0) {
           vSum = scalarMultiply(1.0 / vNb, vSum)
      }
      (docid, vSum)
    })/*.filter(vec => Vectors.norm(vec, 1.0) > 0.0)*/.persist()


    val numTerms = 1000;
    val (termDocMatrix, termIds, docIds, idfs) = com.cloudera.datascience.lsa.ParseWikipedia.termDocumentMatrix(lemmatized, /*stopwordsBroadcast,*/ numTerms, sc);
    termDocMatrix.cache()

    var vSum = termDocMatrix.reduce( (a, b) => add(a, b)) 
    var specificTerms = vSum.toArray.zipWithIndex.sortBy(-_._1).map{ case (tfidf, id) => (termIds(id), tfidf) }

    val mat = new RowMatrix(termDocMatrix)
    val k = 200 // nombre de valeurs singulières à garder
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
