object practice1 {
  /*
   flatMap = map + flat
   在flatMap中，我们会传入一个函数，该函数对每个输入都会返回一个集合（而不是一个元素），
   然后，flatMap把生成的多个集合“拍扁”成为一个集合
   e.g: 此处要求了单个输出为List（必须），但是最终输出的只为一个集合
  */
  def flatMap1(): Unit = {
    val li = List(1, 2, 3)
    val res = li.flatMap(x => x match {
      case 3 => List('a', 'b')
      case _ => List(x * 2)
    })
    println("flatMap1: ")
    println(res)
  }

  def flatMap2(): Unit = {
    val words = List("ABC", "DEF", "GHI")
    val res = words.flatMap(x => x.toList)
    println("flatMap2: used to compare the result with map3")
    println(res)
  }

  // map就是将具体function的内容施加到每个variable上
  def map1(): Unit = {
    val li = List(1, 2, 3)
    val res = li.map(x => x match {
      case 3 => List('a', 'b')
      case _ => x * 2
    })
    println("map1: ")
    println(res)
  }

  def map2(): Unit = {
    val li = List(1, 2, 3)
    val res = li.map(x => x match {
      case 3 => List('a', 'b')
      case _ => List(x * 2)
    })
    println("map2: ")
    println(res)
  }

  def map3(): Unit = {
    val words = List("ABC", "DEF", "GHI")
    val res = words.map(x => x.toList)
    println("map3: used to compare the result with flatMap2")
    println(res)
  }

  def main(args: Array[String]): Unit = {
    flatMap1()
    map1()
    map2()
    flatMap2()
    map3()
  }
}
