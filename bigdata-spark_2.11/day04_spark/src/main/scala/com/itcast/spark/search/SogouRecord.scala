package com.itcast.spark.search

/**
 * @author by FLX
 * @date 2021/6/16 0016 21:09.
 */
case class SogouRecord (
                         queryTime: String,
                         userId: String,
                         queryWords: String,
                         resultRank: Int,
                         clickRank: Int,
                         clickUrl: String
)
