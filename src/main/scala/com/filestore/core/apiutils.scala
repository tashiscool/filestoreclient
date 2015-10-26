package com.filestore.core

import java.awt.image.BufferedImage
import java.io._
import java.util.UUID
import javax.imageio.ImageIO

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.codec.binary.{ Base64, Base64OutputStream }
import play.api.Logger
import play.api.libs.Files.TemporaryFile
import play.api.libs.json.{ JsObject, JsString, _ }

import scalax.io.support.FileUtils

/**
 * Created by tash on 1/22/15.
 */
object ApiUtils {

  def jsonToFile(json: JsValue): Seq[TemporaryFile] = {
    val filesBinaries = json \ "files" \\ "file"
    filesBinaries.map {
      binary =>
        val dataString = binary.as[String]
        val bytes = Base64.decodeBase64(dataString.getBytes("UTF-8"))
        val tempFile = new File(UUID.randomUUID().toString)
        FileUtils.copy(new ByteArrayInputStream(bytes), new FileOutputStream(tempFile))
        TemporaryFile(tempFile)
    }
  }

  def parse(jsValue: JsValue): String = {
    jsValue match {
      case v: JsObject => parse(v).toString()
      case v: JsArray => parse(v).toString()
      case v: JsString => v.value
      case v: JsNumber => v.value.toString
      case v: JsBoolean => v.value.toString
      case JsNull => null
      case v: JsUndefined => null
    }
  }
  def parse(array: JsArray): Seq[String] = {
    array.value map { v =>
      parse(v)
    }
  }

  def parse(map: JsObject): Seq[(String, String)] = {

    map.fields.map { p =>
      (p._1, p._2 match {
        case v: JsObject => {
          parse(v).toString()
        }
        case v: JsArray => { parse(v).toString() }
        case v: JsValue => { parse(v).toString() }
      })
    }
  }

  def jsonToMap(json: JsValue): Map[String, String] = {
    json match {
      case v: JsObject => parse(v).toMap[String, String]
      case _ => Map()
    }
  }

  /**
   * Serialize this model to a JSON object
   *
   * @return  JSON string if the object could be serialized or an empty String if there was a problem.
   */
  def toJson(something: Object): String = {
    try {
      val mapper: ObjectMapper = new ObjectMapper
      val sw: StringWriter = new StringWriter
      mapper.writeValue(sw, something)
      val jsonValue: String = sw.toString
      if (jsonValue.contains("'")) {
        return jsonValue.replaceAll("'", "&#39;").replaceAll("<", "&lt;").replaceAll(">", "&gt;")
      }
      return jsonValue.replaceAll("<", "&lt;").replaceAll(">", "&gt;")
    } catch {
      case e: Exception => {
        Logger.error("Error serializing User object to JSON", e)
        return ""
      }
    }
  }

  def imageToBase64String(image: BufferedImage): String = {
    val os = new ByteArrayOutputStream();
    val b64 = new Base64OutputStream(os);
    ImageIO.write(image, "png", b64);
    val result = os.toString("UTF-8");
    result
  }

  def merge(newMetadata: FileStoreMetadata, oldMetadata1: FileStoreMetadata) = {
    FileStoreMetadata(newMetadata.data.++(oldMetadata1.data))
  }

}
