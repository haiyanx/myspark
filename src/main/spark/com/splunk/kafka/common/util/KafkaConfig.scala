package com.splunk.kafka.common.util

import java.util.HashMap


object KafkaConfig extends Configuration{
  private var conf:HashMap[String,String]=new HashMap[String,String]
  var kafkaParams: Map[String, String]=null
  var topics:Set[String]=null
  var groupid:String=null
  def this(path:String){
    this()
    Configuration.initConf(path, this)

  }
  def this(kp: Map[String, String]){
    this()
    setKafkaParams(kp)
  }
  def this(path:String,kp: Map[String, String]){
    this()
    setKafkaParams(kp)
    Configuration.initConf(path, this)
  }
  def setKafkaParams(kp: Map[String, String]){
    kafkaParams=kp
    this.groupid=kafkaParams.getOrElse("group.id", "test")
  }
  def getKafkaParams()={
    kafkaParams
  }
  def setGroupID(g:String){
    this.groupid=g
  }
  def getGoupid()={
    groupid
  }
  def kpIsNull:Boolean=kafkaParams==null
  def tpIsNull:Boolean=topics==null

  def setTopics(topics:Set[String]){
    this.topics=topics
  }
  def setTopics(topic:String){
    setTopics(topic.split(",").toSet)
  }


}
