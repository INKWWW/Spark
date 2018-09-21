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

  val f : Int => String = myInt => "The value  of myInt is: " + myInt.toString()


  def main(args: Array[String]): Unit = {
    var var_1 : String = "variable_1";
    println(var_1)
//  sum two int
    addInt(1, 2)
//    syntaxCandy
    syntaxCandy(3)
//    test val f
    println(f(8))
  }
}
