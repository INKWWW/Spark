object practice1 {

  // flatMap = map + flat
  def flatMap1(): Unit = {
    val li = List(1, 2, 3)
    val res = li.flatMap(x => x match {
      case 3 => List('a', 'b')
      case _ => List(x * 2)
    })
    println(res)
  }

  // map就是将具体function的内容施加到每个variable上
  def map1(): Unit = {
    val li = List(1, 2, 3)
    val res = li.map(x => x match {
      case 3 => List('a', 'b')
      case _ => x * 2
    })
    println(res)
  }

  def map2(): Unit = {
    val li = List(1, 2, 3)
    val res = li.map(x => x match {
      case 3 => List('a', 'b')
      case _ => List(x * 2)
    })
    println(res)
  }

  def main(args: Array[String]): Unit = {
    println("flatMap1: ")
    flatMap1()
    println("map1: ")
    map1()
    println("map2: ")
    map2()
  }
}
