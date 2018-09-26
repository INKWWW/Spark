object data_structure {

  // Array
  def arrayTest() : Unit = {
    val newString = new Array[String](3) // 注意：newString是val，但是该array里面的具体值是可变的
    // val newString = Array("first, second, third")
    newString(0) = "first, "  // use () instead of []
    newString(1) = "second, "
    newString(2) = "third"
    println("array test: ")
    for (i <- 0 to 2) {
      print(newString(i))
      print("\n")
    }
  }

  // List
  def listTest() : Unit = {
    val newList1 = List(1, 2)
    val newList2 = List(3, 4)
    println(s"newList1: $newList1")  // 此处必须加上 s"xxx"

  }

  def main(args: Array[String]): Unit = {
    arrayTest()
    listTest()
  }

}
