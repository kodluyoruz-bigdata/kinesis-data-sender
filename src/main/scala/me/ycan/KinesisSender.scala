package me.ycan

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesisfirehose.{AmazonKinesisFirehoseAsyncClient, AmazonKinesisFirehoseClient}
import com.amazonaws.services.kinesisfirehose.model.{PutRecordBatchRequest, Record}

import scala.io.Source
import scala.collection.JavaConverters._

object KinesisSender {

  def main(args: Array[String]): Unit = {

    val regionName = Regions.EU_WEST_1.getName
    val streamName = args(1)
    val key        = args(2)
    val secret     = args(3)
    val filePath   = args(4)

    val firehoseClient = AmazonKinesisFirehoseAsyncClient
      .asyncBuilder()
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(key, secret)))
      .withRegion(regionName)
      .build()

    val lines = Source.fromFile(filePath).getLines()
    val groupedLines = lines.grouped(500)

    val records = groupedLines.map{ lines =>
      val records: Seq[Record] = lines.map(line => createRecord(line))
      val batchPutRecords = new PutRecordBatchRequest().withDeliveryStreamName(streamName).withRecords(records.asJavaCollection)
      firehoseClient.putRecordBatch(batchPutRecords)

    }

    records.foreach{results =>
      println(results.getFailedPutCount)

    }



  }

  def createRecord(inputData: String): Record = {

    new Record()

      .withData(ByteBuffer.wrap((inputData + "\n").getBytes))
  }

}
