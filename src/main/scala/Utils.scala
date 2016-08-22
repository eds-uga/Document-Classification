/**
  * Created by UNisar on 8/22/2016.
  */
case class Document(wordCount: Map[Word, Int], labels: Set[TargetClass])
case class DocumentLength(target: TargetClass, length: Double)
case class TargetClass(value: String)
case class Word(value: String)