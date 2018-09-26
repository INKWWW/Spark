object scala_study_1 {

  def addInt(a: Int, b: Int) : Int = {
    var sum = a + b
    println("sum of two Int: ")
    println(sum)
    return sum
  }

  def syntaxCandy(a: Int) : Unit = {
    var result : String = "the value is :" + a.toString();
    println(result);
  }

  val f : Int => String = myInt => "The value  of myInt is: " + myInt.toString();

  def subStr(a: String) : Unit = {
    var result : String = a.substring(0, 3)
    println(result)
  }

  def forLoop(a: Int): Unit = {
//    decide to operate which java program according to the num input
    if (a == 1) {
      for (i <- 1 to 5)
        println(i)
    }
    if (a == 2) {
      for (i <- 1 to 5 if i >1 )
        println(i)
    }
    if (a == 3) {
      for (i <- 1 to 5 if i > 1; if i < 5; if i % 2 == 0 )
        println(i)
    }
  }


  def m1(a: Int, b: Int ) : Int = a * b
  val f1 = m1 _  // 此处 m1空格加下划线就是将方法转换成函数
  print("下划线语法糖-方法转函数：")
  println(f1(2, 3))

  val value1 = (1, 2)
  print("下划线语法糖-tuple: ")
  print(value1)
  print("    value._1: ")
  print(value1._1)
  print("   ")
  print(value1._2)


  def main(args: Array[String]): Unit = {
    var var_1 : String = "variable_1";
    println(var_1)
//  sum two int
    addInt(1, 2)
//    syntaxCandy
    syntaxCandy(3)
//    test val f
    println(f(8))
//    test subString
    subStr("abcdefg")
//    test for loop
    forLoop(3)
  }
}
