object practice2 {
  def operateMap(): Unit = {
    val mapValue = Map("Scala" -> 10, "Java" -> 20, "Ruby" -> 5)
    val res = mapValue.map {case (_, count) => count + 1}
    println(s"operateMap: ${res}")
  }

  // used to compare with "operateMap"
  def operateCollect(): Unit = {
    val mapValue = Map("Scala" -> 10, "Java" -> 20, "Ruby" -> 5)
    val res = mapValue.collect {case (_, count) => count + 1}
    println(s"operateCollect: ${res}")
  }

  def main(args: Array[String]): Unit = {
    operateMap()
    operateCollect()
  }

}
