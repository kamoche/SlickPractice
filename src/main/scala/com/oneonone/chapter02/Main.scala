package com.oneonone.chapter02


import slick.jdbc.H2Profile.api._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object Main extends App {

  lazy val messages = TableQuery[MessageTable]
  val db = Database.forConfig("chapter02")
  val query = messages.map(_.content)
  val pod = query.filter { content: Rep[String] => content like "%pod%" }
  val tupleToSDS = messages.map(t => (t.sender, t.content).mapTo[TextOnly])
  val containBay = for {
    m <- messages
    if m.content like "%bay%"
  } yield m


  exec(messages.schema.create >> (messages ++= freshTestData)) match {
    case Some(value) => println(value + " records inserted")
    case e => println(e)
  }
  val bayMentioned: DBIO[Boolean] = containBay.exists.result

  def exec[T](action: DBIO[T]): T = Await.result(db.run(action), 4 seconds)


  logToConsole(exec(query.result))
  logToConsole(pod.result.statements.mkString)
  logToConsole(exec(pod.result))
  logToConsole(messages.map(t => (t.id, t.content)).result.statements.mkString)
  logToConsole(messages.result.statements.mkString)
  logToConsole(messages.filter(_.sender === "Shaz").result.statements.mkString)


  //make to scala data structure

  def freshTestData = Seq(
    Message("Dave", "Hello, HAL. Do you read me, HAL?"),
    Message("HAL", "Affirmative, Dave. I read you."),
    Message("Dave", "Open the pod bay doors, HAL."),
    Message("HAL", "I'm sorry, Dave. I'm afraid I can't do that.")
  )

  def logToConsole(message: Any): Unit = {
    print(s"\n${message} \n")
  }



  logToConsole(tupleToSDS.result)

  logToConsole(messages.map(t => t.id * 1000L).result.statements.mkString)

  logToConsole(exec(messages.map(t => (t.id * 1000L, t.content)).result))
  logToConsole(exec(messages.map(t => (t.id, t.content)).result))


  //exist

  final case class Message(sender: String, content: String, id: Long = 0)

  logToConsole(containBay.result.statements.mkString)

  final class MessageTable(tag: Tag) extends Table[Message](tag, "MESSAGE") {

    val id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    val sender = column[String]("SENDER")
    val content = column[String]("CONTENT")

    override def * = (sender, content, id).mapTo[Message]
  }

  logToConsole(bayMentioned)

  logToConsole(messages.filter(_.sender === "Dave").result.statements.mkString)
  logToConsole(messages.filter(_.sender =!= "Dave").result.statements.mkString)
  logToConsole(messages.filter(_.sender < "HAL").result.statements.mkString)
  logToConsole(messages.filter(m => m.sender >= m.content).result.statements.mkString)

  case class TextOnly(sender: String, content: String)

  //String concatination
  logToConsole("String concat")
  logToConsole(messages.map(m => m.sender ++ "> " ++ m.content).result.statements.mkString)

  //sorting
  logToConsole("Sorting")
  exec(messages.sortBy(_.content).result).foreach(println)
  //reverse sort
  logToConsole("Reverse sorting")
  exec(messages.sortBy(_.sender.desc).result).foreach(println)

  logToConsole("Sorting by multiple column")
  logToConsole(messages.sortBy(m => (m.sender, m.content)).result.statements.mkString)

  exec(messages.sortBy(m => (m.sender, m.content)).result).foreach(println)

  logToConsole("Sort and take 2")
  exec(messages.sortBy(_.sender).take(2).result).foreach(println)

  logToConsole("pagination")
  logToConsole(messages.sortBy(_.sender).drop(5).take(5).result.statements.mkString)

  logToConsole("Count messages")
  logToConsole(messages.length.result.statements.mkString)
  logToConsole(exec(messages.length.result))

  logToConsole("-----------Exercise------------")

  logToConsole("---select message using for comprehension---")

  val messageWithID1= for {
    m <- messages
    if m.id === 1L
  } yield m

  logToConsole(exec(messageWithID1.result))

  val messageWithIDOne_1=messages.filter(_.id === 1L)

  logToConsole(exec(messageWithIDOne_1.result))

  logToConsole("Filter by ID sql statement: " + messages.filter(_.id === 1L).result.statements.mkString)


  logToConsole("---Is HAL Real?---")
  logToConsole(exec(messages.filter(_.sender === "HAL").exists.result))

  logToConsole("---Selecting Columns---")
  logToConsole(exec(messages.map(m => m.content).result))

  logToConsole("---First Result---")

  logToConsole(exec(messages.filter(_.sender === "HAL").result.head))
  logToConsole(exec(messages.filter(_.sender === "Alice").result.headOption))

  logToConsole("---2.10.8 Then the Rest---")
  logToConsole(exec(messages.filter(_.sender === "HAL").drop(1).take(5).result))

  logToConsole("---2.10.10 Liking---")
  val likingQuery=messages.filter(_.content.toLowerCase like "%do%").result
  logToConsole("Like sql statement: "+ likingQuery.statements.mkString)
  logToConsole(exec(likingQuery))

  logToConsole("---2.10.11 Client-Side or Server-Side?---")
  logToConsole(exec(messages.map(_.content ++ "!").result))
}
