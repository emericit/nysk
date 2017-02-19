/* Analyse des articles de presse sur l'afaire DSK */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd

import scala.xml._

import org.apache.hadoop.io.{ Text, LongWritable }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

import com.cloudera.datascience.common.XmlInputFormat
import com.cloudera.datascience.lsa.ParseWikipedia._
//import com.cloudera.datascience.lsa.RunLSA._

import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors, Matrix}
import org.apache.spark.mllib.linalg.distributed.{RowMatrix}
import org.apache.spark.mllib.linalg.SingularValueDecomposition
//import org.apache.spark.mllib.clustering.KMeans
//import org.apache.spark.mllib.util.KMeansDataGenerator


import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV, DenseMatrix}
import org.apache.spark.mllib.linalg.{Vector => SparkVector}

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.io._

object NYSK_w2vec {

  def toBreeze(v:SparkVector) = BV(v.toArray)
  def fromBreeze(bv:BV[Double]) = Vectors.dense(bv.toArray)
  def add(v1:SparkVector, v2:SparkVector) = fromBreeze(toBreeze(v1) + toBreeze(v2))
  def scalarMultiply(a:Double, v:SparkVector) = fromBreeze(a * toBreeze(v))

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
  //   - on extrait le champ "docId"
  def extractDocId(elem: scala.xml.Elem): Long = {
    val dn: scala.xml.NodeSeq = elem \\ "docid"
    val x: Long = dn.text.toLong
    return x
  }

  // Pour un élément XML de type "document",
  //   - on extrait le champ "source"
  def extractSource(elem: scala.xml.Elem): String = {
    val dn: scala.xml.NodeSeq = elem \\ "source"
    val x: String = dn.text
    return x
  }

  def extractAll(elem: scala.xml.Elem): (Long, java.sql.Timestamp, String, String, String, String) = {
    return (extractDocId(elem), extractDate(elem), extractSource(elem), extractTitle(elem), extractSummary(elem), extractText(elem))
  }

  // Nécessaire, car le type java.sql.Timestamp n'est pas ordonné par défaut (étonnant...)
  implicit def ordered: Ordering[java.sql.Timestamp] = new Ordering[java.sql.Timestamp] {
    def compare(x: java.sql.Timestamp, y: java.sql.Timestamp): Int = x compareTo y
  }

  def xml2rdd (sc: SparkContext, path: String): RDD[(Long, java.sql.Timestamp, String, String, String, String)] = {
    val nysk_xml: RDD[Elem] = loadArticle(sc, path).map(XML.loadString)
    //val nysk_timestamps: RDD[java.sql.Timestamp] = nysk_xml.map(extractDate)
    val nysk: RDD[(Long, java.sql.Timestamp, String, String, String, String)] = nysk_xml.map(extractAll)
    nysk
  }

  def rdd2wordtovec (sc: SparkContext, lemmatized: RDD[Seq[String]]):
        RowMatrix = {  
    val stopwords = sc.textFile("/user/emeric/stop_words").collect.toArray.toSet
    val bStopwords = sc.broadcast(stopwords)

    // lire le Word2VecModel
    val w2vModel = Word2VecModel.load(sc, "w2vModel")

    // obtenir une Map[String, Array[Float]] sérializable   //   mapValues seul ne retourne pas une map sérializable (SI-7005)
    val vectors = w2vModel.getVectors.mapValues(vv => Vectors.dense(vv.map(_.toDouble))).map(identity)

    // transmettre la map aux noeuds de calcul
    val bVectors = sc.broadcast(vectors)

    // taille des vecteurs Word2Vec dans le modèle chargé
    val vectSize = 100

    // calcul des représentations Word2Vec des textes d'articles
    val matLignes: RowMatrix = new RowMatrix (
      lemmatized.map( wordSeq => {
        var vSum = Vectors.zeros(vectSize)
        var vNb = 0
        wordSeq.foreach { word =>
            if(!(bStopwords.value)(word) & (word.length >= 2)) {
                bVectors.value.get(word).foreach { v =>
                    vSum = add(v, vSum)
                    vNb += 1
                }
            }
        }
        if (vNb != 0) {
            vSum = scalarMultiply(1.0 / vNb, vSum)
        }
        vSum
      }).filter( vec => Vectors.norm(vec, 1.0) > 0.0 )
    )
    matLignes;
  }

  def lemmatize(sc: SparkContext, nysk: RDD[(Long, java.sql.Timestamp, String, String, String, String)]): RDD[Seq[String]] = {
    val stopwords = sc.textFile("/user/emeric/stop_words").collect.toArray.toSet
    val bStopwords = sc.broadcast(stopwords)
    // mapPartitions pour ne pas initialiser le NLPPIpeline 1 fois par élément du RDD
    // mais une fois par noeud de calcul  
    val lemmatized = nysk.mapPartitions(iter => {
      val pipeline = com.cloudera.datascience.lsa.ParseWikipedia.createNLPPipeline();
      iter.map {
        case (docid, date, source, title, summary, text) =>
            com.cloudera.datascience.lsa.ParseWikipedia.plainTextToLemmas(text, bStopwords.value, pipeline)
      };
    })
    lemmatized;
  }
    
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("NYSK")
    val sc = new SparkContext(conf)
    val nysk = xml2rdd (sc, "/user/emeric/nysk.xml")
    println ("***** " + nysk.count() + " articles loaded")
    val lemmatized = lemmatize(sc, nysk)
    val w2vec = rdd2wordtovec(sc, lemmatized)
    println ("***** " + w2vec.numRows + " word2vec constructed")
    val w2vectopics = w2vec.rows.map(x => w2vModel.findSynonyms(x,5))
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = w2vec.computeSVD(100, computeU = true)
    val U: RowMatrix = svd.U // The U factor is a RowMatrix.
    val s: Vector = svd.s // The singular values are stored in a local dense vector.
    val V: Matrix = svd.V // The V factor is a local dense matrix.
    val pca3 = w2vec.computePrincipalComponents(3)
    val projected = w2vec.multiply(pca3)
    val rdd = projected.rows.map( x => x.toArray.mkString(","))
    val pca3mat = new DenseMatrix (100, 3, pca3.toArray)
    val filepath = "/user/emeric/projection3D.txt"
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://head.local:8020"), hadoopConf)
    try { hdfs.delete(new org.apache.hadoop.fs.Path(filepath), true) } catch { case _ : Throwable => { } }
    rdd.saveAsTextFile(filepath)
    val pw =  new PrintWriter(new File("/home/emeric/nysk/eigen_values.txt" ))
    for (x <- s.toArray) {
      pw.write(x*x + "\n")  
    }
    pw.close()
    sc.stop()
  }
}
