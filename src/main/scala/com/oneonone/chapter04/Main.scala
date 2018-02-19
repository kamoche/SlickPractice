package com.oneonone.chapter04

import com.oneonone.chapter03.Main.Message
import slick.jdbc.H2Profile.api._

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.util.Try

object Main extends App {

  lazy val messages = TableQuery[MessageTable]
  val db = Database.forConfig("chapter04")
  val freshData = List(
    Message("Bob", "Hi Alice"),
    Message("Alice", "Hi Bob"),
    Message("Bob", "Are you sure this is secure?"),
    Message("Alice", "Totally, why do you ask?"),
    Message("Bob", "Oh, nothing, just wondering."),
    Message("Alice", "Ten was too many messages"),
    Message("Bob", "I could do with a sleep"),
    Message("Alice", "Let's just to to the point"),
    Message("Bob", "Okay okay, no need to be tetchy."),
    Message("Alice", "Humph!"))
  val populateData = messages ++= freshData

  def logToConsole(message: Any): Unit = {
    print(s"\n${message}\n")
  }

  def exec[T](action: DBIO[T]): T = Await.result(db.run(action), 4.seconds)

  final case class Message(sender: String, content: String, id: Long = 0L)


  logToConsole("-----------------------> Combining Actions <------------------------")

  final class MessageTable(tag: Tag) extends Table[Message](tag, "MESSAGE") {

    val id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    val sender = column[String]("SENDER")
    val content = column[String]("CONTENT")

    def * = (sender, content, id).mapTo[Message]
  }

  try {
    logToConsole("Create schema and populate data")

    exec(DBIO.seq(messages.schema.create, populateData))

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

    val reverseText: DBIO[Option[String]] = text.map(optionText => optionText.map(_.reverse))
    logToConsole("Reverse text: " + exec(reverseText))

    logToConsole("---> 4.2.4 DBIO.successful and DBIO.failed <---")

    DBIO.successful(100)
    DBIO.failed(new RuntimeException("Some nasty errors"))

    logToConsole("---> 4.2.5 flatMap <---")

    val delete = messages.delete

    def insert(count: Int) = messages += Message("King", s"Deleted ${count} messages")

    val resetMessageAction = delete.flatMap(count => insert(count))

    logToConsole("Message reset count: " + exec(resetMessageAction))

    exec(messages.result).foreach(println)


    val resetMessageAction1: DBIO[Int] =
      delete.flatMap {
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

    val reports: List[DBIO[Int]] = report1 :: report2 :: Nil

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

    logToConsole("---> 4.2.9 andFinally and cleanUp <---")


    def log(err: Throwable): DBIO[Int] = {
      messages += Message("SYSTEM", err.getMessage)
    }

    val work = DBIO.failed(new RuntimeException("Boom"))

    val action1: DBIO[Int] = work.cleanUp {
      case Some(err) => log(err)
      case None => DBIO.successful(0)
    }

    exec(action1.asTry)

    logToConsole(exec(messages.filter(_.sender === "SYSTEM").result))


    logToConsole("--> 4.2.10 asTry <---")

    logToConsole(exec(work.asTry))

    logToConsole("Records size: " + exec(messages.size.result.asTry))

    logToConsole("---> 4.3 Logging Queries and Results <---")

    logToConsole("---> 4.4 Transactions <---")

    def updateContent(old: String) = messages.filter(_.content === old).map(_.content)

    val transactionalAction =
      (updateContent("Let's just to to the point").update("Wanna come in?") >>
        updateContent("I could do with a sleep").update("Pretty please!") >>
        updateContent("Ten was too many messages").update("Opening now.")).transactionally

    exec(transactionalAction)

    exec(messages.result).foreach(println)


    val withRollBack =
      (updateContent("Wanna come in?").update("bab baba ?") >>
        updateContent("Pretty please!").update("caution!") >>
        DBIO.failed(new Exception("Wait for it")) >>
        updateContent("Opening now.").update("slightly ahead.")).transactionally
    logToConsole("------------>Transaction with roll back<----------------------")
    exec(withRollBack.asTry)

    exec(messages.result).foreach(println)


    logToConsole("----------------------> 4.6 Exercise <------------------------")
    logToConsole("---> 4.6.1 And Then what? <--- ")

    val dropSchema = messages.schema.drop
    val createSchema = messages.schema.create
    val populate = messages ++= freshData

    logToConsole("Combine dropSchem >> createSchema >> populateFreshData: " + exec(dropSchema >> createSchema >> populate))

    val allInAll = DBIO.seq(dropSchema, createSchema, populate)

    logToConsole("---> 4.6.2 First! <---")


    def insert1(m: Message): DBIO[Int] = {
      messages.size.result.flatMap {
        case 0 => (messages += Message(m.sender, "First!")) >> (messages += m)
        case n => (messages += m)

      }
    }

    exec(messages.delete)
    exec(insert1(Message("Ambrose", "hit First!")))

    exec(messages.result).foreach(println)

    logToConsole("---> 4.6.3 There Can be Only One <---")

    def onlyOne[T](action: DBIO[Seq[T]]): DBIO[T] = action.flatMap { ms =>
      ms match {
        case m +: Nil => DBIO.successful(m)
        case ys => DBIO.failed(
          new RuntimeException(s"Expected 1 result but not ${ys.length}")
        )
      }

    }

    val happy = messages.filter(_.content like "%hit%").result
    val boom = messages.filter(_.content like "%First%").result

    //    logToConsole(exec(onlyOne(happy)))
    //    logToConsole(exec(onlyOne(boom)))

    logToConsole("---> 4.6.4 Let’s be Reasonable <---")

    def exactlyOne[T](action: DBIO[Seq[T]]): DBIO[Try[T]] = onlyOne(action).asTry

    logToConsole(exec(exactlyOne(happy)))
    logToConsole(exec(exactlyOne(boom)))

    logToConsole("---> 4.6.5 Filtering <---")

    def myFilter[T](action: DBIO[T])(p: T => Boolean)(alternative: => T) = action.map {
      case t if p(t) => t
      case _ => alternative
    }

    myFilter(messages.size.result)(_ > 20)(200)

    final case class Room(name: String, connectsTo: String)

    final class FloorPlan(tag: Tag) extends Table[Room](tag, "floorplan") {
      def name = column[String]("name")

      def connectsTo = column[String]("next")

      def * = (name, connectsTo).mapTo[Room]
    }

    lazy val floorplan = TableQuery[FloorPlan]

    exec {
      (floorplan.schema.create) >>
        (floorplan += Room("Outside","Podbay Door")) >>
        (floorplan += Room("Podbay Door", "Podbay")) >>
        (floorplan += Room("Podbay", "Galley")) >>
        (floorplan += Room("Galley", "Computer")) >>
        (floorplan += Room("Computer", "Engine Room"))
    }

    def unfold(z: String,f: String => DBIO[Option[String]], acc: Seq[String] = Seq.empty ): DBIO[Seq[String]] = {
      f(z).flatMap {
        case None
        => DBIO.successful(acc :+ z)
        case Some(r) => unfold(r, f, acc :+ z)
      }
    }
  } finally db.close()


}
