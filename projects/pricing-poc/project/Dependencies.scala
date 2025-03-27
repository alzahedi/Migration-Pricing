import sbt._

object Dependencies {
  val sparkVersion = "3.3.0"
  val hadoopVersion = "3.3.1"
  val azureIdentityVersion = "1.4.5"
  val azureStorageBlobVersion = "12.23.0"

  val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion
  val sparkSQL = "org.apache.spark" %% "spark-sql" % sparkVersion
  val hadoopAzure = "org.apache.hadoop" % "hadoop-azure" % hadoopVersion
  val hadoopCommon = "org.apache.hadoop" % "hadoop-common" % hadoopVersion
  val hadoopAzureDatalake =
    "org.apache.hadoop" % "hadoop-azure-datalake" % hadoopVersion
  val azureIdentity = "com.azure" % "azure-identity" % azureIdentityVersion
  val azureStorageBlob =
    "com.azure" % "azure-storage-blob" % azureStorageBlobVersion

  lazy val munit = "org.scalameta" %% "munit" % "0.7.29"
}
