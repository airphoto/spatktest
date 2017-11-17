package com.lhs

import org.apache.log4j.{Level, Logger}

/**
 * Created by Administrator on 2017/1/13.
 */

/**
 * <P>@ObjectName : ImplicitDefDemo</P>
 *
 * <P>@USER : abel.li </P>
 *
 * <P>@CREATE_AT : 2017/1/13 </P>
 *
 * <P>@DESCRIPTION : TODO </P>
 */

object ImplicitDefDemo {
  Logger.getLogger("org").setLevel(Level.ERROR)
  object MyImplicitTypeConversion{
    implicit def str2Int(str:String) = str.toInt
  }
  def main(args: Array[String]) {
//    import MyImplicitTypeConversion.str2Int
//    val max = math.max("1",2)
//    print(max)
  }
}
