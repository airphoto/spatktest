package com.lhs.spark.dataset

object Messages {

  case class GamePlay(tag:String,
                      endTime:String,
                      gameID:String,
                      playID:String,
                      options:Array[String],
                      playTimes:String,
                      factTimes:String,
                      dismiss:String,
                      userIDs:Array[String],
                      mIP:String,
                      mPort:String,
                      mID:String,
                      startTime:String,
                      tableID:String,
                      gameType:String,
                      roundBase:String,
                      cards:String,
                      factCards:String,
                      version:String)

}
