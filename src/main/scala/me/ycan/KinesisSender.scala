package me.ycan

import java.nio.ByteBuffer

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.model.PutRecordRequest

import scala.io.Source

object KinesisSender {

  def main(args: Array[String]): Unit = {

    val regionName = args(0)
    val streamName = args(1)
    val key        = args(2)
    val secret     = args(3)
    val filePath   = args(4)

    val kinesisClient = AmazonKinesisClientBuilder
      .standard()
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(key, secret)))
      .withRegion(regionName)
      .build()

    val records = Source.fromFile(filePath).getLines()

    records.foreach { r =>
      val record = createPutRecord(streamName, 1, r)
      val result = kinesisClient.putRecord(record)
      println("Put Result: " + result)
    }

  }

  def createPutRecord(streamName: String, partitionKey: Int, inputData: String): PutRecordRequest = {

    new PutRecordRequest()
      .withStreamName(streamName)
      .withPartitionKey(partitionKey.toString)
      .withData(ByteBuffer.wrap((inputData + "\n").getBytes))
  }

}
