import org.json4s.JsonDSL._

val input = "Hey it was a greatp match #Boxing #Mayweather"

val x = ("Text" -> input) ~
  ("HashTags" -> input.split(" ").filter(_.startsWith("#")).toList)