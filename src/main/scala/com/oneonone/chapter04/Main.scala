package com.oneonone.chapter04

import com.oneonone.chapter03.Main.Message
import slick.jdbc.H2Profile.api._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.Await

object Main extends App{

   case class Message(sender: String, content: String, id: Long = 0L)

    class MessageTable(tag: Tag) extends Table[Message](tag, "MESSAGE"){

      val id=column[Long]("ID", O.PrimaryKey, O.AutoInc)
      val sender=column[String]("SENDER")
      val content=column[String]("CONTENT")

      def * = (sender,content, id).mapTo[Message]
    }



  def logToConsole(message: Any): Unit ={
    print(s"\n${message}\n")
  }
  val db=Database.forConfig("chapter04")

  val messages=TableQuery[MessageTable]

  def exec[T](action: DBIO[T]): T = Await.result(db.run(action), 4.seconds)

  val freshData = List(
    Message("Bob", "Hi Alice"),
    Message("Alice","Hi Bob"),
    Message("Bob","Are you sure this is secure?"),
    Message("Alice","Totally, why do you ask?"),
    Message("Bob","Oh, nothing, just wondering."),
    Message("Alice","Ten was too many messages"),
    Message("Bob","I could do with a sleep"),
    Message("Alice","Let's just to to the point"),
    Message("Bob","Okay okay, no need to be tetchy."),
    Message("Alice","Humph!"))


  logToConsole("-----------------------> Combining Actions <------------------------")

  val populateData = messages ++= freshData

  logToConsole("Create schema and populate data")

  exec(messages.schema.create >> populateData)

  logToConsole("------------------> INSERTED RECONDS <------------------------------")
  exec(messages.result).foreach(println)


  logToConsole("---> 4.2. COMBINATORS IN DETAIL <---")

  val resetData: DBIO[Int] = messages.delete >> messages.size.result

  logToConsole("Size of records after deletion: " + exec(resetData))

  logToConsole("---> 4.2.2 DBIO.seq <---")

  val resetData1: DBIO[Unit] = DBIO.seq(messages.delete, messages.size.result)

  logToConsole("Combine action and disregard everything: " + exec(resetData1))

  logToConsole("---> 4.2.3 map <---")

  //restore deleted data
  exec(populateData)

  import scala.concurrent.ExecutionContext.Implicits.global

  val text: DBIO[Option[String]] = messages.map(_.content).result.headOption

  logToConsole("TEXT:  " + exec(text))

  val reverseText: DBIO[Option[String]]=text.map(optionText => optionText.map(_.reverse))
  logToConsole("Reverse text: " + exec(reverseText) )

  logToConsole("---> 4.2.4 DBIO.successful and DBIO.failed <---")

  DBIO.successful(100)
  DBIO.failed(new RuntimeException("Some nasty errors"))

  logToConsole("---> 4.2.5 flatMap <---")

  val delete=messages.delete

  def insert(count: Int) = messages +=Message("King", s"Deleted ${count} messages")

  val resetMessageAction= delete.flatMap(count => insert(count))

  logToConsole("Message reset count: " + exec(resetMessageAction))

  exec(messages.result).foreach(println)


  val resetMessageAction1: DBIO[Int] =
    delete.flatMap{
      case 0 => DBIO.successful(0)
      case n => insert(n)
    }


  logToConsole("---> 4.2.6 DBIO.sequence <---")

  def reverse(msg: Message): DBIO[Int] =
    messages.filter(_.id === msg.id).
      map(_.content).
      update(msg.content.reverse)

  val updates: DBIO[Seq[Int]] =
    messages.result.
      flatMap(msgs => DBIO.sequence(msgs.map(reverse)))

  logToConsole("---> 4.2.7 DBIO.fold <---")

  val report1: DBIO[Int] = DBIO.successful(41)

  val report2: DBIO[Int] = DBIO.successful(1)

  val reports: List[DBIO[Int]] =report1 :: report2 :: Nil

  val default: Int = 0

  val summary: DBIO[Int] =
    DBIO.fold(reports, default) {
      (total, report) => total + report
    }

  exec(summary)


  logToConsole("---> 4.2.8 zip <---")

  val zip: DBIO[(Int, Seq[Message])] = messages.size.result zip messages.filter(_.sender === "Bob").result

  exec(messages ++= freshData)

  logToConsole("Zip action returns both result: " + exec(zip))


}
