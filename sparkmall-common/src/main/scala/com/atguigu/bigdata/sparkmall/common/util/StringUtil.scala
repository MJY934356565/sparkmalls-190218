package com.atguigu.bigdata.sparkmall.common.util

object StringUtil {
  def isNotEmpty(s: String): Boolean = {
    s != null && !"".equals(s.trim)

  }

}
