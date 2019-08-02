object MyString {
  def main(args: Array[String]): Unit = {
   var str = "AaDd"
   var str1 = "BbCc"
   val flag = str.compareToIgnoreCase("aadd")
   println(str.compareTo("AAaa"))
   
   println(flag)
  }
}