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
import org.apache.spark.mllib.feature.StandardScaler

import java.io.StringReader
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.process.CoreLabelTokenFactory;
import edu.stanford.nlp.process.DocumentPreprocessor;
import edu.stanford.nlp.process.PTBTokenizer;

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.mllib.clustering.{ KMeans, KMeansModel }
import org.apache.spark.mllib.util.KMeansDataGenerator

object NYSK {

  def toBreeze(v:Vector) = BVector(v.toArray)
  def fromBreeze(bv:BVector[Double]) = Vectors.dense(bv.toArray)
  def add(v1:Vector, v2:Vector) = fromBreeze(toBreeze(v1) + toBreeze(v2))
  def scalarMultiply(a:Double, v:Vector) = fromBreeze(a * toBreeze(v))
  def stackVectors(v1:Vector, v2:Vector) = {
    var v3 = Vectors.zeros(v1.size+v2.size)
          for (i <- 0 until v1.size) {
              BVector(v3.toArray)(i) = v1(i);
          }
          for (i <- 0 until v2.size) {
              BVector(v3.toArray)(v1.size+i) = v2(i);
          }
    v3
  }

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
  
  def hasLetters(str: String): Boolean = {
    // While loop for high performance
    var i = 0
    while (i < str.length) {
      if (Character.isLetter(str.charAt(i))) {
        return true
      }
      i += 1
    }
    false
  }

  def main(args: Array[String]) {
    var textToExtract = "text";
    var useW2Vec = true;
    var weightTfIdf = true;
    println ("*****");
    for (s <- args) {
        s match {
            case "w2vec" => useW2Vec = true;
            case "now2vec" => useW2Vec = false;
            case "weighttfidf" => weightTfIdf = true;
            case "weight1" => weightTfIdf = false;
            case "text" => textToExtract = "text";
            case "title" => textToExtract = "title";
            case "summary" => textToExtract = "summary";
            case _ => println("Unrecognized option: " + s.toString);
        }
    }
    println("Extracting " + textToExtract);
    if (useW2Vec) {
        println("Using w2vec with weights " + { if (weightTfIdf) "TfIdf" else "1" });
    }
    else {
        println("No w2vec");
    }
    println ("*****");
    val conf = new SparkConf().setAppName("NYSK")
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)

    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://localhost:9000"), hadoopConf)
    val nysk_raw = loadArticle(sc, "hdfs://head.local:9000/user/emeric/nysk.xml")/*.sample(false,0.01)*/
    val nysk_xml: RDD[Elem] = nysk_raw.map(XML.loadString)
    //val nysk_timestamps: RDD[java.sql.Timestamp] = nysk_xml.map(extractDate)
    /*println(nysk_raw.count() + " articles covering " + nysk_timestamps.min() + " to " + nysk_timestamps.max())*/
    //val nysk_texts: RDD[String] = nysk_xml.map(extractText)
    /*println(nysk_texts.count())*/
    val nysk: RDD[(Int, java.sql.Timestamp, String)] = nysk_xml.map(e => extractAll(e,textToExtract))
    val nyskTitles: RDD[(Int, java.sql.Timestamp, String)] = nysk_xml.map(e => extractAll(e,"title"))
    val nyskSummaries: RDD[(Int, java.sql.Timestamp, String)] = nysk_xml.map(e => extractAll(e,"summary"))

    val stopwords = sc.textFile("hdfs://head.local:9000/user/emeric/stopwords.txt").collect.toArray.toSet
    val stopwordsBroadcast = sc.broadcast(stopwords).value

    // mapPartitions pour ne pas initialiser le NLPPIpeline 1 fois par élément du RDD
    // mais une fois par noeud de calcul      
    val lemmatizedWithDate = nysk.mapPartitions(iter => {
      val pipeline = com.cloudera.datascience.lsa.ParseWikipedia.createNLPPipeline();
      iter.map {
        case (docid, date, text) =>
          (docid.toString, date,
            com.cloudera.datascience.lsa.ParseWikipedia.plainTextToLemmas(text.toLowerCase.split("\\W+").mkString(" "), stopwordsBroadcast, pipeline))
        };
    })
    val lemmatized = lemmatizedWithDate.map { case (docid, date, text) => (docid, text) }
    val numTerms = 1000;
    val (termDocMatrix, termIds, docIds, idfs) = com.cloudera.datascience.lsa.ParseWikipedia.termDocumentMatrix(lemmatized, stopwordsBroadcast, numTerms, sc);
    termDocMatrix.cache();

    if (! useW2Vec) {
        val mat = new RowMatrix(termDocMatrix)
        val k = 10 // nombre de valeurs singulières à garder
        val svd = mat.computeSVD(k, computeU=true)
        val projections = mat.multiply(svd.V)
        val projectionsTxt = projections.rows.map(l => l.toString.filter(c => c != '[' & c != ']'))
        // Delete the existing path, ignore any exceptions thrown if the path doesn't exist
        val outputProjection = "hdfs://head.local:9000/user/emeric/projection_LSA.txt"
        try { hdfs.delete(new org.apache.hadoop.fs.Path(outputProjection), true) } 
        catch { case _ : Throwable => { } }
        projectionsTxt.saveAsTextFile(outputProjection)
        
        val nbClusters = 10
        val nbIterations = 1000
        val runs = 10
        val clustering = KMeans.train(termDocMatrix, nbClusters, nbIterations, runs, "k-means||", 0)
        /*val outputClustering = "hdfs://head.local:9000/user/emeric/clusters"
        try { hdfs.delete(new org.apache.hadoop.fs.Path(outputClustering), true) } 
        catch { case _ : Throwable => { } }
        clustering.save(sc, outputClustering)*/
        
        val classes = clustering.predict(termDocMatrix)
        val outputClasses = "hdfs://head.local:9000/user/emeric/classes_LSA.txt"
        try { hdfs.delete(new org.apache.hadoop.fs.Path(outputClasses), true) } 
        catch { case _ : Throwable => { } }
        classes.saveAsTextFile(outputClasses)
        
        val outputData = lemmatizedWithDate.zip(classes).map { case ((docid, date, title),cl) => (docid, date, cl) }.sortBy(_._2).map(l => l.toString.filter(c => c != '(' & c != ')'))
        val outputDataFile = "hdfs://head.local:9000/user/emeric/output_LSA.txt"
        try { hdfs.delete(new org.apache.hadoop.fs.Path(outputDataFile), true) } 
        catch { case _ : Throwable => { } }
        outputData.saveAsTextFile(outputDataFile)
        
        clustering.clusterCenters.foreach(clusterCenter => {
            val highest = clusterCenter.toArray.zipWithIndex.sortBy(-_._1).map(v => v._2).take(10)
            println("*****")
            highest.foreach { s => print( termIds(s) + "," ) }
            println ()
            }
       )
    }
    else {
        val w2vModel = Word2VecModel.load(sc, "hdfs://head.local:9000/user/emeric/w2vModel")
        // obtenir une Map[String, Array[Float]] sérializable
        //   mapValues seul ne retourne pas une map sérializable (SI-7005)
        val vectors = w2vModel.getVectors.mapValues(vv => org.apache.spark.mllib.linalg.Vectors.dense(vv.map(_.toDouble))).map(identity)
        // transmettre la map aux noeuds de calcul
        val bVectors = sc.broadcast(vectors)

        val w2vModel2 = Word2VecModel.load(sc, "hdfs://head.local:9000/user/emeric/w2vModel2")
        // obtenir une Map[String, Array[Float]] sérializable
        //   mapValues seul ne retourne pas une map sérializable (SI-7005)
        val vectors2 = w2vModel2.getVectors.mapValues(vv => org.apache.spark.mllib.linalg.Vectors.dense(vv.map(_.toDouble))).map(identity)
        // transmettre la map aux noeuds de calcul
        val bVectors2 = sc.broadcast(vectors2)

        
        val idTerms = termIds.map(_.swap)

        val pairs = termDocMatrix.zip(lemmatized);

        var w2vecRepr = pairs.map({ case (row, (docid, lemmas)) => 
          var vSum = Vectors.zeros(100)
          var totalWeight: Double = 0
          val words = lemmas.toSet
          words.foreach { word =>
               val colId = idTerms.get(word);
               if (colId != None) {
                   var weight = row(colId.get);
                   if (! weightTfIdf) {
                       weight = 1.;
                   }
                   val wvec = bVectors2.value.get(word)
                   if (wvec != None) {          
                       vSum = add(scalarMultiply(weight, wvec.get), vSum)
                       totalWeight += weight
                   }
               }
          }
          if (totalWeight > 0) {
               vSum = scalarMultiply(1.0 / totalWeight, vSum)
          }
          else {
               vSum = Vectors.zeros(100);
          }
          (docid, vSum)
        })/*.filter(vec => Vectors.norm(vec._2, 1.0) > 0.0)*/.persist() 
        val matRDD = w2vecRepr.map{v => v._2}.cache()
        val mat = new RowMatrix(matRDD)
        val matrixTxt = mat.rows.map(l => l.toString.filter(c => c != '[' & c != ']'))
        // Delete the existing path, ignore any exceptions thrown if the path doesn't exist
        val outputMatrix = "hdfs://head.local:9000/user/emeric/matrice_W2V.txt"
        try { hdfs.delete(new org.apache.hadoop.fs.Path(outputMatrix), true) } 
        catch { case _ : Throwable => { } }
        matrixTxt.saveAsTextFile(outputMatrix)

        val centRed = new StandardScaler(withMean = true, withStd = true).fit(matRDD)
        val matCR: RowMatrix = new RowMatrix(centRed.transform(matRDD))

        val matCompPrinc = matCR.computePrincipalComponents(10)
        val projections = matCR.multiply(matCompPrinc)
        //val matSummary = projections.computeColumnSummaryStatistics()
        val projectionsTxt = projections.rows.map(l => l.toString.filter(c => c != '[' & c != ']'))
        // Delete the existing path, ignore any exceptions thrown if the path doesn't exist
        val outputProjection = "hdfs://head.local:9000/user/emeric/projection_W2V.txt"
        try { hdfs.delete(new org.apache.hadoop.fs.Path(outputProjection), true) } 
        catch { case _ : Throwable => { } }
        projectionsTxt.saveAsTextFile(outputProjection)
        
        val nbClusters = 10
        val nbIterations = 1000
        val runs = 10
        val clustering = KMeans.train(matRDD, nbClusters, nbIterations, runs, "k-means||", 0)
        /*val outputClustering = "hdfs://head.local:9000/user/emeric/clusters"
        try { hdfs.delete(new org.apache.hadoop.fs.Path(outputClustering), true) } 
        catch { case _ : Throwable => { } }
        clustering.save(sc, outputClustering)*/
        
        val classes = clustering.predict(matRDD)
        val outputClasses = "hdfs://head.local:9000/user/emeric/classes_W2V.txt"
        try { hdfs.delete(new org.apache.hadoop.fs.Path(outputClasses), true) } 
        catch { case _ : Throwable => { } }
        classes.saveAsTextFile(outputClasses)
        
        val outputData = lemmatizedWithDate.zip(classes).map { case ((docid, date, title),cl) => (docid, date, cl) }.sortBy(_._2).map(l => l.toString.filter(c => c != '(' & c != ')'))
        val outputDataFile = "hdfs://head.local:9000/user/emeric/output_W2V.txt"
        try { hdfs.delete(new org.apache.hadoop.fs.Path(outputDataFile), true) } 
        catch { case _ : Throwable => { } }
        outputData.saveAsTextFile(outputDataFile)
        
       clustering.clusterCenters.foreach(clusterCenter => {
            val nearest = w2vecRepr.map{v => (v._1, Vectors.sqdist(v._2,clusterCenter))}.sortBy(_._2).map{ case (id, dist) => id }.take(10).toSet
            println("*****")
            pairs.filter{ case (row, (docid, lemmas)) => nearest contains docid }.foreach {  case (row, (docid, lemmas)) => {
                val id = row.toArray.zipWithIndex.sortBy(- _._1).take(5).map(_._2)
                id.foreach { s => print( termIds(s) + "," ) }
                println ()
                }
            }
        }
       )
        
        /*val factors = sc.textFile("hdfs://head.local:9000/user/emeric/factors.txt")
        val matP = Matrices.dense(numRows=100, numCols=4, values=factors.map(line => line.split(" ").map(w => w.toDouble)).collect.flatten)
        val projectionsP = mat.multiply(matP)
        val projectionsPTxt = projectionsP.rows.map(l => l.toString.filter(c => c != '[' & c != ']'))
        // Delete the existing path, ignore any exceptions thrown if the path doesn't exist
        val outputProjectionP = "hdfs://head.local:9000/user/emeric/projectionP.txt"
        try { hdfs.delete(new org.apache.hadoop.fs.Path(outputProjectionP), true) } 
        catch { case _ : Throwable => { } }
        projectionsPTxt.saveAsTextFile(outputProjectionP) */
    }
    /*val numTerms = 1000;
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
    */
    sc.stop()
  }
}
