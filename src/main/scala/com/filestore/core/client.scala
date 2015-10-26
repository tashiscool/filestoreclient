
package com.filestore.core

import java.io.{ ByteArrayInputStream, File, InputStream }
import java.util.{ Date, UUID }
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec

import akka.util.ReentrantGuard
import com.filestore.core.ApiUtils._
import com.filestore.core.ClientImplicits.{ FileStoreMetadataReader, FileStoreMetadataWriter }
import com.typesafe.config.Config
import fly.play.aws.auth.{ AwsCredentials, SimpleAwsCredentials }
import fly.play.s3._
import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang3.StringUtils
import org.joda.time.format.ISODateTimeFormat
import org.slf4j.LoggerFactory
import play.api.Play.current
import play.api.http.Status
import play.api.libs.iteratee._
import play.api.libs.json._
import play.api.libs.ws.WS
import reactivemongo.api._
import reactivemongo.api.collections.default.BSONCollection
import reactivemongo.api.gridfs._
import reactivemongo.bson._
import reactivemongo.core.commands.{ Count, LastError }
import reactivemongo.utils._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ ExecutionContext, Future }
import scala.language.implicitConversions
import scala.util.{ Failure, Success }

object ClientImplicits {

  val queryableFields = List("mdn", "email", "partyId", "portalUserId", "url", "productInstanceId", "user", "product", "session", "plan", "temporary", "S3", "state", "result")

  implicit object BsonObjectIdWrites extends Writes[BSONObjectID] {
    override def writes(o: BSONObjectID): JsValue = Json.obj("id" -> o.stringify)
  }
  implicit object BsonObjectIdReads extends Reads[BSONObjectID] {
    import reactivemongo.bson._
    override def reads(json: JsValue): JsResult[BSONObjectID] = json match {
      case js: JsString => JsSuccess(BSONObjectID(js.value))
      case _ => JsError()
    }

  }

  implicit object FileStoreMetadataWrites extends Writes[BSONDocument] {
    override def writes(doc: BSONDocument): JsValue = {
      val something = queryableFields.foldLeft(List[(String, Json.JsValueWrapper)]()) { (returnedMap, field) =>
        returnedMap.::((field, JsString(doc.getAs[String](field).getOrElse(""))))
      }

      val somethingElse = doc.getAs[String]("mergedFields").getOrElse("").split("\\|").toList.foldLeft(List[(String, Json.JsValueWrapper)]()) { (returnedMap, field) =>
        if (!queryableFields.contains(field)) returnedMap.::((field, JsString(doc.getAs[String](field).getOrElse("")))) else returnedMap
      }
      Json.obj(something.++(somethingElse): _*)
    }
  }

  implicit object FileStoreMetadataReads extends Reads[BSONDocument] {
    override def reads(json: JsValue): JsResult[BSONDocument] = {
      JsSuccess(parse(json.asInstanceOf[JsObject]))
    }
    private def parse(map: JsObject): BSONDocument = {

      val something: Seq[(String, BSONValue)] = map.fields.map { p =>
        (p._1, p._2 match {
          case v: JsObject =>
            specialMongoJson(v).fold(
              normal => parse(normal),
              {
                case d: Date => BSONDateTime(d.getTime)
                case s: String => parse(JsString(s))
              }
            )
          case v: JsArray => parse(v)
          case v: JsValue => parse(v)
        })
      }
      BSONDocument(something)
    }
    private def specialMongoJson(json: JsObject): Either[JsObject, Object] = {
      if (json.fields.nonEmpty) {
        json.fields.headOption match {
          case Some((k, v: JsString)) if k == "$date" => Right(try { new Date(v.value.toLong) } catch { case e: Exception => ISODateTimeFormat.dateTime().parseDateTime(v.value).toDate })
          case Some((k, v: JsNumber)) if k == "$date" => Right(new Date(v.value.toLong))
          case Some((k, v: JsString)) if k == "$oid" => Right(v.value)
          case Some((k, v)) if k.startsWith("$") => throw new RuntimeException("unsupported specialMongoJson " + k + " with v: " + v.getClass + ":" + v.toString())
          case _ => Left(json)
        }
      } else Left(json)
    }

    private def parse(array: JsArray): BSONValue = {
      val stuff = array.value.map(v => parse(v))
      BSONArray(stuff)
    }

    def parse(v: JsValue): BSONValue = v match {
      case v: JsObject => parse(v)
      case v: JsArray => parse(v)
      case v: JsString => BSONString(v.value)
      case v: JsNumber => BSONDouble(v.value.toLongExact)
      case v: JsBoolean => BSONBoolean(v.value)
      case JsNull => null
      case v: JsUndefined => null
    }
  }

  implicit val fileWrites = Json.writes[FileStoreFile]
  implicit val fileReads = Json.reads[FileStoreFile]

  implicit object FileListWrites extends Writes[List[FileStoreFile]] {
    override def writes(o: List[FileStoreFile]): JsValue = o.foldRight(JsArray())((newFile, aggregator) => aggregator :+ Json.toJson(newFile))
  }

  implicit object FileStoreMetadataReader extends BSONDocumentReader[FileStoreMetadata] {
    override def read(doc: BSONDocument): FileStoreMetadata = {

      val queryable = queryableFields.foldLeft(Map[String, String]()) { (returnedMap, field) =>
        returnedMap.+((field, doc.getAs[String](field).getOrElse("")))
      }
      val mergedFields = doc.getAs[String]("mergedFields").getOrElse("").split("\\|").toList.foldLeft(Map[String, String]()) { (returnedMap, field) =>
        if (!queryableFields.contains(field)) returnedMap.+((field, doc.getAs[String](field).getOrElse(""))) else returnedMap
      }
      FileStoreMetadata(queryable.++(mergedFields))
    }
  }

  implicit object FileStoreMetadataWriter extends BSONDocumentWriter[FileStoreMetadata] {
    override def write(t: FileStoreMetadata): BSONDocument = {
      val something = {
        t.data.foldLeft(List[(String, BSONValue)]()) { case (returnedList, (k, v)) => returnedList.::((k, BSONString.apply(v))) }
      }
      val mergedFields = ("mergedFields", BSONString(t.data.keySet.mkString("|")))
      BSONDocument(something.::(mergedFields))
    }
  }

  implicit object DefaultFileWriter extends BSONDocumentWriter[DefaultFileToSave] {
    override def write(file: DefaultFileToSave): BSONDocument = {
      val chunkSize = 0
      val length = 0
      BSONDocument(
        "_id" -> file.id.asInstanceOf[BSONValue],
        "filename" -> BSONString(file.filename),
        "chunkSize" -> BSONInteger(chunkSize),
        "length" -> BSONInteger(length),
        "uploadDate" -> BSONDateTime(file.uploadDate.getOrElse(new Date().getTime)),
        "contentType" -> file.contentType.map(BSONString).getOrElse(throw new IllegalArgumentException("contentType Is Required")),
        "metadata" -> option(!file.metadata.isEmpty, file.metadata).getOrElse(throw new IllegalArgumentException("contentType Is Required"))
      )
    }
  }

  implicit object DefaultFileToSaveReader extends BSONDocumentReader[DefaultFileToSave] {
    import DefaultBSONHandlers._
    def read(doc: BSONDocument) = {
      DefaultFileToSave(
        doc.getAs[BSONString]("filename").map(_.value).get,
        doc.getAs[BSONString]("contentType").map(_.value),
        doc.getAs[BSONNumberLike]("uploadDate").map(_.toLong),
        doc.getAs[BSONDocument]("metadata").getOrElse(BSONDocument()),
        doc.getAs[BSONValue]("_id").get
      )
    }
  }

  implicit object FileStoreFileReader extends BSONDocumentReader[FileStoreFile] {
    import reactivemongo.bson.DefaultBSONHandlers._
    override def read(doc: BSONDocument): FileStoreFile = {
      FileStoreFile(
        doc.getAs[BSONObjectID]("_id").get,
        doc.getAs[BSONString]("contentType").map(_.value),
        doc.getAs[BSONString]("filename").map(_.value).get,
        doc.getAs[BSONDateTime]("uploadDate").map(_.value),
        doc.getAs[BSONNumberLike]("chunkSize").map(_.toInt).get,
        doc.getAs[BSONNumberLike]("length").map(_.toInt).get,
        doc.getAs[BSONString]("md5").map(_.value),
        doc.getAs[BSONDocument]("metadata").getOrElse(BSONDocument())
      )
    }
  }

  implicit def readFile2FileStoreFile[Id <: BSONValue](readFile: ReadFile[Id]): FileStoreFile = {
    FileStoreFile(
      readFile.id.asInstanceOf[BSONObjectID],
      readFile.contentType,
      readFile.filename,
      readFile.uploadDate,
      readFile.chunkSize,
      readFile.length,
      readFile.md5,
      readFile.metadata
    )
  }

  implicit def formToFileStoreMetadata(form: Map[String, Seq[String]]): FileStoreMetadata = {
    FileStoreMetadata(form.foldLeft[Map[String, String]](Map[String, String]()) { case (returnedMap, (k, v)) => returnedMap.+((k, v.head)) })
  }

  implicit def jsonToFileStoreMetadata(jsValue: JsObject): FileStoreMetadata = {
    FileStoreMetadata(parse(jsValue).toMap[String, String])
  }

}

class Client(val gfs: GridFS[BSONDocument, BSONDocumentReader, BSONDocumentWriter])(implicit val ec: ExecutionContext) {

  import com.filestore.core.ClientImplicits._

  val MAX_RESULTS = 20

  def logger = LoggerFactory.getLogger(Client.getClass)

  def remove(id: String): Future[Boolean] = {
    gfs.remove(BSONObjectID(id)) map {
      lastError =>
        lastError.errMsg match {
          case Some(error) =>
            logger.error(s"Error removing file $id")
            false
          case None =>
            true
        }
    }
  }

  def updateMetadata(query: Map[String, Seq[String]], updatedMetadata: Map[String, String], append: Boolean, maxResults: Int = MAX_RESULTS) = {
    queryFiles(query, maxResults).flatMap { images =>
      Future.sequence {
        images.map { retrievedFile =>
          enumerate(retrievedFile).run(Iteratee.consume[Array[Byte]]()).flatMap { byteArray =>
            val retrievedMetadata = retrievedFile.metadata
            remove(retrievedFile.id.stringify) flatMap { result =>
              val fileStoremetadata = if (append) { FileStoreMetadata(FileStoreMetadataReader.read(retrievedMetadata).data.++(updatedMetadata)) } else { FileStoreMetadata(updatedMetadata) }
              save(Some(retrievedFile.id.stringify), Right(new ByteArrayInputStream(byteArray)), retrievedFile.filename, retrievedFile.contentType.getOrElse("jpg"), fileStoremetadata) recover {
                case ex: Exception =>
                  logger.error("file saving ~may~ have failed ", ex)
                  Left(ex.getMessage)
              }
            }
          }
        }
      }
    }
  }

  def enumerate(file: ReadFile[_ <: BSONValue]) = gfs.enumerate(file)

  def save(id: Option[String], eitherFileorStream: Either[File, InputStream], filename: String, contentType: String, metadata: FileStoreMetadata): Future[Either[FileStoreFile, String]] = {
    import reactivemongo.api.gridfs.Implicits._

    val newId = id.map(v => BSONObjectID(v)).getOrElse(BSONObjectID.generate)
    val fileToSave = DefaultFileToSave(filename, Some(contentType), Some(System.currentTimeMillis()), FileStoreMetadataWriter.write(metadata), newId)
    val futureSaved = eitherFileorStream match {
      case Left(file) =>
        gfs.save(Enumerator.fromFile(file), fileToSave).map {
          readFile =>
            logger.debug(s"Wrote file $filename successfully")
            Left(readFile2FileStoreFile(readFile))
        }
      case Right(stream) =>
        gfs.save(Enumerator.fromStream(stream), fileToSave).map {
          readFile =>
            logger.debug(s"Wrote file $filename successfully")
            Left(readFile2FileStoreFile(readFile))
        }
    }
    futureSaved recover {
      case e: Exception =>
        logger.error(s"Error writing file $filename ", e)
        Right("Error writing file")
    }
  }

  def save(id: Option[String], enumerator: Enumerator[Array[Byte]], filename: String, contentType: String, metadata: FileStoreMetadata): Future[Either[FileStoreFile, String]] = {
    import reactivemongo.api.gridfs.Implicits._

    val newId = id.map(v => BSONObjectID(v)).getOrElse(BSONObjectID.generate)
    val fileToSave = DefaultFileToSave(filename, Some(contentType), Some(System.currentTimeMillis()), FileStoreMetadataWriter.write(metadata), newId)
    val futureSaved = gfs.save(enumerator, fileToSave).map {
      readFile =>
        logger.debug(s"Wrote file $filename successfully")
        Left(readFile2FileStoreFile(readFile))
    }
    futureSaved recover {
      case e: Exception =>
        logger.error(s"Error writing file $filename ", e)
        Right("Error writing file")
    }
  }

  def getFile(id: String): Future[Option[FileStoreFile]] = {
    gfs.find[BSONDocument, FileStoreFile](BSONDocument("_id" -> BSONObjectID(id))).headOption(ec)
  }

  def queryFiles(query: Map[String, Seq[String]], maxResults: Int = MAX_RESULTS): Future[List[FileStoreFile]] = {
    gfs.find[BSONDocument, FileStoreFile](Client.buildSelector(query)).collect[List](maxResults)
  }

}

class GFSReactiveMetadata(val db: DB, val client: Client, val config: Config)(implicit val ec: ExecutionContext) {
  import DefaultBSONHandlers._
  import com.filestore.core.ClientImplicits.DefaultFileToSaveReader

  def logger = LoggerFactory.getLogger(Client.getClass)
  val MAX_UPDATES = 100
  val MAX_RESULTS = 20
  val MAX_retry = 3
  val encrypted: Boolean = try { config.getBoolean("aws.encrypted") } catch { case e: Throwable => logger.error("aws.encrypted not found", e); false }

  trait CryptoKeyService {
    def keyVal(config: Config) = {
      config.getString("aws.encryptionKey")
    }
  }

  val keyService: CryptoKeyService = new CryptoKeyService {}

  val md = java.security.MessageDigest.getInstance("MD5")
  val userCollectionNameString: String = "fs.files"
  def collection = db.collection[BSONCollection](userCollectionNameString)
  val lock = new ReentrantGuard
  var buckets = Client.enableAWS(config)
  var retryCounter = 0

  def reloadBucket(config: Config): Option[Future[Bucket]] = lock.withGuard {
    if (retryCounter < MAX_retry) {
      buckets = Client.enableAWS(config)
      retryCounter = retryCounter + 1
      buckets
    } else {
      logger.warn("reached max retries")
      None
    }
  }
  def bucketOption = {
    while (lock.isLocked) {

    }
    buckets
  }

  private val _id: String = "_id"
  private val mergedfields: String = "mergedFields"
  private val metadata1: String = "metadata"
  private val $set: String = "$set"
  private val s3: String = "S3"
  private val s3enabled: String = s3 + "enabled"

  def countArbitaryQuerySet(query: Map[String, JsValue]): Future[Int] = {

    val queryParams = BSONDocument(
      query.foldLeft(List[(String, BSONValue)]()) {
        case (returnedMap, (key, value)) =>
          returnedMap.::((key, ClientImplicits.FileStoreMetadataReads.parse(value)))
      }
    )
    val command = Count(userCollectionNameString, Some(queryParams))
    val result = db.command(command) // returns Future[Int]

    result
  }

  def remove(id: String, retryCounter: Int = 0): Future[Boolean] = {
    getFile(id).flatMap {
      case Some(file) =>
        val s3FilenameOption = FileStoreMetadataReader.read(file.metadata).data.get(s3)
        bucketOption match {
          case Some(bucket) =>
            s3FilenameOption match {
              case Some(s3Filename) if (StringUtils.isNotBlank(s3Filename)) =>
                bucket.flatMap {
                  _.remove(s3Filename).map {
                    _ => true
                  }
                } recoverWith {
                  case error =>
                    logger.warn("issue with bucket", error)
                    reloadBucket(config)
                    if (retryCounter < MAX_retry) {
                      logger.debug(s"Retrying removing file ${file.filename} from s3. Retry count $retryCounter")
                      remove(id, retryCounter + 1)
                    } else {
                      logger.debug(s"Max retries reached for removing file ${file.filename}")
                      Future.successful(false)
                    }
                }
              case _ => Future(false)

            }
          case _ => Future(false)
        }
      case _ => Future(false)
    } flatMap {
      case true => client.remove(id)
      case false => Future(false)
    }
  }

  def updateMetadata(query: Map[String, Seq[String]], updatedMetadata: Map[String, String], append: Boolean): Future[List[LastError]] = {
    logger.debug(s"Updating metadata for all files that match query $query")
    collection.find(com.filestore.core.Client.buildSelector(query)).cursor[BSONDocument].collect[List](MAX_UPDATES).flatMap {
      resultset: List[BSONDocument] =>
        Future.sequence {
          resultset.map { result =>
            val objectId = result.getAs[BSONObjectID](_id).get
            val newMetaMap = if (append) {
              val mappedData = FileStoreMetadataReader.read(result.getAs[BSONDocument](metadata1).getOrElse(BSONDocument())).data.++(updatedMetadata)
              val mergedFields = (mergedfields, mappedData.keySet.filterNot(_ == mergedfields).mkString("|"))
              mappedData + mergedFields
            } else {
              updatedMetadata.+((mergedfields, updatedMetadata.keySet.filterNot(_ == mergedfields).mkString("|")))
            }
            // create a modifier document, ie a document that contains the update operations to run onto the documents matching the query
            val modifier = BSONDocument(
              // this modifier will set the fields 'updateDate', 'title', 'content', and 'publisher'
              $set -> BSONDocument(
                metadata1 -> BSONDocument(
                  newMetaMap.foldLeft(List[(String, BSONValue)]()) {
                    case (returnedMap, (key, value)) =>
                      returnedMap.::((key, BSONString(value)))
                  }
                )
              )
            )
            // ok, let's do the update
            logger.debug(s"Updating metadata for file with ID ${objectId.stringify}")
            collection.update(BSONDocument(_id -> objectId), modifier).andThen {
              case Success(lastError) if lastError.ok =>
                logger.debug(s"Successfully updated metadata for file with ID ${objectId.stringify}")
                Success(lastError)
              case Success(lastError) =>
                logger.error(s"Failed to update metadata for file with ID ${objectId.stringify}: $lastError")
                Success(lastError)
              case Failure(t) =>
                logger.error(s"Failed to update metadata for file with ID ${objectId.stringify}", t)
                Failure(t)
            }
          }
        }
    }
  }

  def updateMetadata(id: String, updatedMetadata: Map[String, String]): Future[LastError] = {
    collection.find(BSONDocument(_id -> BSONObjectID(id))).cursor[DefaultFileToSave].collect[List](1).flatMap { resultset =>
      Future.sequence {
        resultset.map {
          result =>
            val objectId = result.id
            val newMetaMap = {
              val mappedData = FileStoreMetadataReader.read(result.metadata).data.++(updatedMetadata)
              val mergedFields = (mergedfields, mappedData.keySet.filterNot(_ == mergedfields).mkString("|"))
              mappedData + mergedFields
            }

            // create a modifier document, ie a document that contains the update operations to run onto the documents matching the query
            val modifier = BSONDocument(
              // this modifier will set the fields 'updateDate', 'title', 'content', and 'publisher'
              $set -> BSONDocument(
                metadata1 -> BSONDocument(
                  newMetaMap.foldLeft(List[(String, BSONValue)]()) {
                    case (returnedMap, (key, value)) =>
                      returnedMap.::((key, BSONString(value)))
                  }
                )
              )
            )
            // ok, let's do the update
            collection.update(BSONDocument(_id -> BSONObjectID(id)), modifier)
        }

      }.map { x =>
        val f = LastError(BSONDocument(_id -> BSONObjectID(id))) match {
          case Right(e) => e
          case _ => throw new IllegalArgumentException("Last error changed type")
        }
        x.headOption.getOrElse(f)
      }
    }
  }

  def fileToSaveToFileStore(file: DefaultFileToSave, btyes: Option[String]) = {
    val newIdTyped = file.id match {
      case id: BSONObjectID => id.stringify
      case string: BSONString => string.value
      case any => any.toString
    }
    FileStoreFile(
      BSONObjectID(newIdTyped),
      file.contentType, file.filename, file.uploadDate, 0, 0,
      btyes.map { l => new String(md.digest(l.getBytes("UTF-8"))) },
      file.metadata
    )

  }

  def getFile(id: String): Future[Option[FileStoreFile]] = {
    collection.find(BSONDocument("_id" -> BSONObjectID(id))).cursor[DefaultFileToSave].collect[List](1).map { lister =>
      lister.map {
        file =>
          val btyes = FileStoreMetadataReader.read(file.metadata).data.get(s3)
          fileToSaveToFileStore(file, btyes)
      }.headOption
    }
  }

  def queryFiles(query: Map[String, Seq[String]], maxResults: Int = MAX_RESULTS, sortBy: Option[String] = None): Future[List[FileStoreFile]] = {
    val queryB = sortBy match {
      case Some(sortkey) => collection.find(Client.buildSelector(query)).sort(BSONDocument( sortkey -> BSONInteger(-1)))
      case None => collection.find(Client.buildSelector(query))
    }
    queryB.cursor[DefaultFileToSave].collect[List](maxResults).map { lister =>
      lister.map {
        file =>
          val btyes = FileStoreMetadataReader.read(file.metadata).data.get(s3)
          fileToSaveToFileStore(file, btyes)
      }
    }
  }

  def enumerate(file: ReadFile[_ <: BSONValue], retryCounter: Int = 0): Future[Enumerator[Array[Byte]]] = {
    val btyes = FileStoreMetadataReader.read(file.metadata).data.get(s3)
    val fileEncrypted = FileStoreMetadataReader.read(file.metadata).data.get(s3enabled) match { case Some(e) => e.toBoolean case _ => false }
    val fileId = file.id match {
      case id: BSONObjectID => id.stringify
      case string: BSONString => string.value
      case any => any.toString
    }

    val key = fileId.substring(0, 16)

    val emptyArray = Enumerator.fromStream(new ByteArrayInputStream(Array.empty[Byte]))
    btyes match {
      case Some(s3Filename) if (StringUtils.isNotBlank(s3Filename)) => bucketOption match {
        case Some(bucket) =>
          bucket.flatMap {
            _.get(s3Filename).flatMap {
              case BucketFile(name, contentType, content, acl, headers) => fileEncrypted match {
                case true =>
                  val decryptedByteArrayFuture = Client.decryptByteArray(content, key)
                  decryptedByteArrayFuture.map { decryptedByteArray =>
                    Enumerator.fromStream(new ByteArrayInputStream(decryptedByteArray))
                  }
                case false => Future.successful(Enumerator.fromStream(new ByteArrayInputStream(content)))
              }
              case _ =>
                logger.warn(s"File ${file.filename} has s3Filename $s3Filename but no bucket configuration was found")
                Future.successful(emptyArray)
            } recoverWith {
              case error =>
                logger.error(s"Error reading file ${file.filename} from s3", error)
                reloadBucket(config)
                if (retryCounter < MAX_retry) {
                  logger.debug(s"Retrying reading file ${file.filename} from s3. Retry count $retryCounter")
                  enumerate(file, retryCounter + 1)
                } else {
                  logger.debug(s"Max retries reached for reading file ${file.filename}")
                  Future.successful(emptyArray)
                }

            }
          }
        case None =>
          logger.warn(s"Error reading file ${file.filename} from s3. No bucket is configured")
          Future.successful(emptyArray)
      }
      case _ =>
        bucketOption match {
          case Some(bucket) =>
            logger.debug(s"Transfering file ${file.filename} to s3")
            transferToS3(fileId, key)
          case None =>
            logger.warn(s"File ${file.filename} should be transferred to s3 but no bucket is configured")
            Future.successful(emptyArray)
        }
    }

  }

  def transferToS3(fileId: String, key: String): Future[Enumerator[Array[Byte]]] = {
    val gfsFile: Future[Option[FileStoreFile]] = client.getFile(fileId)
    gfsFile.flatMap {
      case Some(fileStoreFile) =>
        val futureEnumerator = client.enumerate(fileStoreFile).run(Iteratee.consume[Array[Byte]]())
        val s3fileName = generateS3FileName(fileStoreFile.filename)
        futureEnumerator.flatMap {
          byteArray =>
            saveToS3AndUpdateMetadata(fileId, fileStoreFile.contentType, s3fileName, byteArray)
            Future.successful(Enumerator.fromStream(new ByteArrayInputStream(byteArray)))
        }
      case None =>
        logger.warn(s"Problem transferring file with ID $fileId to S3. No metadata found in mongo")
        Future.successful(Enumerator.fromStream(new ByteArrayInputStream(Array.empty[Byte])))

    }
  }

  def saveToS3AndUpdateMetadata(fileId: String, contentType: Option[String], s3fileName: String, byteArray: Array[Byte]): Future[Any] = {
    logger.debug(s"Transferring file with ID $fileId and s3 filename $s3fileName")
    s3Save(fileId.substring(0, 16), s3fileName, contentType.getOrElse("img/jpg"), byteArray).map {
      case true =>
        logger.debug(s"Successfully saved file with ID $fileId and s3 filename $s3fileName to S3")
        updateMetadata(fileId, Map[String, String](s3 -> s3fileName, s3enabled -> s"$encrypted")).map { x =>
          Option(x) match {
            case Some(result) if !result.ok =>
              logger.debug(s"Transferring file with ID $fileId and s3 filename $s3fileName but metadata update failed: $result")
              bucketOption.getOrElse(throw new scala.IllegalArgumentException("unable to get bucket")).flatMap(_.remove(s3fileName)).onComplete {
                case _ => logger.error(s"revert failed, check mongo ${result.message}, check s3 orphan $s3fileName")
              }
            case _ => logger.debug(s"Transfer complete for file with ID $fileId and s3 filename $s3fileName")
          }
        }
      case false => logger.error(s"Transfer complete for file with ID $fileId and s3 filename $s3fileName")
    }
  }

  def save(id: Option[String], eitherFileorStream: Either[File, InputStream], filename: String, mimeType: String, metadata: FileStoreMetadata, acl: Option[fly.play.s3.ACL] = Some(fly.play.s3.AUTHENTICATED_READ), headers: Option[Map[String, String]] = None): Future[Either[FileStoreFile, String]] = {
    import com.filestore.core.ClientImplicits.DefaultFileWriter
    val byteArrayEnum = eitherFileorStream match {
      case Left(file) =>
        Enumerator.fromFile(file)
      case Right(stream) =>
        Enumerator.fromStream(stream)

    }
    byteArrayEnum.run(Iteratee.consume[Array[Byte]]()).flatMap { byteArray =>
      val newId = id.map(v => BSONObjectID(v)).getOrElse(BSONObjectID.generate)
      val s3filename = generateS3FileName(filename)
      val timestamp = Some(System.currentTimeMillis())
      val s3EnabledMetadata = if (byteArray.nonEmpty) { FileStoreMetadataWriter.write(FileStoreMetadata(metadata.data.+((s3, s3filename)).+((s3enabled, s"$encrypted")))) } else { FileStoreMetadataWriter.write(FileStoreMetadata(metadata.data)) }
      collection.save(DefaultFileToSave(filename, Some(mimeType), timestamp, s3EnabledMetadata, newId)).flatMap { lastError =>
        if (lastError.ok) {
          s3Save(newId.stringify.substring(0, 16), s3filename, mimeType, byteArray, acl, headers).map {
            case true => Left(FileStoreFile(newId, Some(mimeType), filename, timestamp, 0, 0, None, s3EnabledMetadata))
            case _ => if (byteArray.nonEmpty) { Right("failed to " + s3 + " Save") } else { Left(FileStoreFile(newId, Some(mimeType), filename, timestamp, 0, 0, None, s3EnabledMetadata)) }
          }
        } else {
          Future(Right("Mongo failed to " + s3 + " Save"))
        }

      }

    }
  }

  private def generateS3FileName(filename: String) = {
    UUID.randomUUID.toString
  }

  def s3Save(key: String, fileName: String, mimeType: String, byteArray: Array[Byte], acl: Option[fly.play.s3.ACL] = Some(fly.play.s3.AUTHENTICATED_READ), headers: Option[Map[String, String]] = None, retryCounter: Int = 0): Future[Boolean] = {
    logger.debug(s"Saving file $fileName to S3 with key $key")
    bucketOption match {
      case Some(bucket) if byteArray.nonEmpty =>
        val byteArrayResultFuture: Future[Array[Byte]] = if (!encrypted) {
          Future.successful(byteArray)
        } else {
          Client.encryptByteArray(byteArray, key)
        }
        val result = byteArrayResultFuture.flatMap {
          byteArrayResult =>
            bucket.flatMap {
              _ add BucketFile(fileName, mimeType, byteArrayResult, acl, headers)
            }.map {
              _ =>
                logger.debug(s"Saved file $fileName to S3 with key $key")
                true
            } recoverWith {
              case error =>
                logger.error(s"Error saving file $fileName with key $key", error)
                if (retryCounter < MAX_retry) {
                  logger.debug(s"Retrying S3 save for file $fileName with key $key. Retry number $retryCounter")
                  reloadBucket(config)
                  s3Save(key, fileName, mimeType, byteArray, acl, headers, retryCounter + 1)
                } else {
                  logger.debug(s"Max retries reached for file $fileName with key $key. Aborting")
                  Future.successful(false)
                }

            }
        }
        result.map {
          case true =>
            logger.info(s"Saved the file $fileName with key $key")
            true
          case false =>
            false
        } recover {
          case S3Exception(status, code, message, originalXml) =>
            logger.error(s"Error saving file $fileName with key $key. S3Exception(status=$status, code=$code, message=$message)")
            false
        }
      case Some(bucket) =>
        logger.warn(s"Error saving file $fileName with key $key. No data to save")
        Future.successful(false)
      case None =>
        logger.error(s"Error saving file $fileName with key $key. No bucket is configured")
        Future.successful(false)
    }
  }

}

object Client {
  import com.filestore.core.ClientImplicits._

  def logger = LoggerFactory.getLogger(Client.getClass)

  def apply(config: Config)(implicit ec: ExecutionContext): Client = {
    val gfs = _initClient(config)
    new Client(gfs)
  }

  def applyMetadata(config: Config)(implicit ec: ExecutionContext): GFSReactiveMetadata = {
    val gfs = _initClientDB(config)
    val gfs2 = _initClient(config)
    new GFSReactiveMetadata(gfs, new Client(gfs2), config)
  }

  def enableAWS(config: Config): Option[Future[Bucket]] = {
    Option(config.getString("aws.bucket")) match {
      case Some(bucketName) =>
        Option(config.getString("aws.accessKeyId")) match {
          case Some(keyId) =>
            Option(config.getString("aws.secretKey")) match {
              case Some(key) =>
                val credentials = AwsCredentials(keyId, key)
                Some(Future(S3(bucketName)(credentials)))
              case _ => None
            }
          case _ => Option(config.getString("aws.role")) match {
            case Some(role) =>
              val credentials = getCreds(role)
              Some(credentials.map(S3(bucketName)(_)))
            case _ => None
          }
        }
      case _ => None
    }
  }

  private def getCreds(role: String): Future[SimpleAwsCredentials] = {
    WS.url(s"http://169.254.169.254/latest/meta-data/iam/security-credentials/$role").get.map {
      response =>
        {
          if (response.status != Status.OK) throw new RuntimeException(response.statusText)
          else {
            val creds = response.json
            SimpleAwsCredentials(
              (creds \ "AccessKeyId").as[String],
              (creds \ "SecretAccessKey").as[String],
              Some((creds \ "Token").as[String])
            )
          }
        }
    }
  }

  private def _initClientDB(config: Config)(implicit ec: ExecutionContext) = {
    val driver = new MongoDriver()
    Option(config.getString("mongodb.uri")).map(MongoConnection.parseURI).flatMap {
      case Success(uri) =>
        Some(Success(driver.connection(uri).db("filestore")))
      case Failure(t) =>
        Some(Failure(new IllegalArgumentException(s"Invalid mongo URI: ${config.getString("mongodb.uri")}", t)))
    }.get match {
      case Success(db) =>
        logger.debug(s"Connected to URI ${config.getString("mongodb.uri")} successfully")
        db
      case Failure(t) =>
        logger.error(s"Failed to connect to URI ${config.getString("mongodb.uri")}", t)
        throw t
    }
  }

  private def _initClient(config: Config)(implicit ec: ExecutionContext) = {
    val db = _initClientDB(config)
    new GridFS[BSONDocument, BSONDocumentReader, BSONDocumentWriter](db)
  }

  def buildSelector(queryString: Map[String, Seq[String]]): BSONDocument = {
    //TODO This is a hack to fix a prod bug asap. Need to fix it properly -DSH
    logger.debug(s"Building selector from query string $queryString")
    if (queryString.keys.exists(_.equals("_id"))) {
      logger.debug(s"Query contains _id, returning basic _id query")
      BSONDocument("_id" -> BSONObjectID(queryString("_id").headOption.getOrElse("")))
    } else if (!queryableFields.exists { x => queryString.keys.exists(y => y == x) }) {
      throw new scala.IllegalArgumentException(s"keys not supported $queryString.keys")
    } else {
      BSONDocument(
        queryableFields.foldLeft(List[(String, BSONValue)]()) { (returnedMap, field) =>
          if (queryString.contains(field)) {
            returnedMap.::(("metadata." + field, BSONString(queryString(field).headOption.getOrElse(""))))
          } else {
            returnedMap
          }
        }
      )
    }
  }

  def encryptByteArray(array: Array[Byte], key: String): Future[Array[Byte]] = Future {
    val cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
    val secretKey = new SecretKeySpec(key.getBytes("UTF-8"), "AES");
    cipher.init(Cipher.ENCRYPT_MODE, secretKey);
    val encryptedString = Base64.encodeBase64String(cipher.doFinal(array));
    encryptedString.getBytes("UTF-8")
  }(Contexts.cryptoContext)

  def decryptByteArray(array: Array[Byte], key: String): Future[Array[Byte]] = Future {
    val strToDecrypt = new String(array, "UTF-8")
    val cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING");
    val secretKey = new SecretKeySpec(key.getBytes("UTF-8"), "AES");
    cipher.init(Cipher.DECRYPT_MODE, secretKey);
    cipher.doFinal(Base64.decodeBase64(strToDecrypt))
  }(Contexts.cryptoContext)
}

case class FileStoreMetadata(data: Map[String, String])

case class FileStoreFile(
  id: BSONObjectID,
  contentType: Option[String],
  filename: String,
  uploadDate: Option[Long],
  chunkSize: Int,
  length: Int,
  md5: Option[String],
  metadata: BSONDocument
) extends ReadFile[BSONObjectID]

