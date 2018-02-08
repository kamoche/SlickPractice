package com.oneonone.chapter03


import slick.jdbc.H2Profile.api._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object Main extends App {


  logToConsole("Creating and Modifying Data")

  def logToConsole(message: Any): Unit = {
    print(s"\n${message}\n")
  }

  final case class Message(sender: String, content: String, id: Long = 0L)

  final class MessageTable(tag: Tag) extends Table[Message](tag, "MESSAGE") {

    val id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    val sender = column[String]("SENDER")
    val content = column[String]("CONTENT")


    override def * = (sender, content, id).mapTo[Message]
  }

  val messages=TableQuery[MessageTable]

  //establish db connection
  val db= Database.forConfig("chapter03")

  def exec[T](action: DBIO[T]): T= Await.result(db.run(action), 4.seconds)

  //create schema
  logToConsole("---> create schema <---")
  exec(messages.schema.create)

  logToConsole("---> 3.1.1 Inserting Single Rows <---")
  val insertOneRecord = messages += Message("Adrian", " Hey dad, want to ride my bike outside")
  logToConsole("Records inserted: " + exec(insertOneRecord))

  logToConsole("Specify own primary key using forceinsert")
  val forceInsertPK= messages forceInsert  Message("Kamoche","Go ahead son, see you later", 3L)
  logToConsole("ForceInsert PK record inserted: " + exec(forceInsertPK))

  logToConsole("---> 3.1.3 Retrieving Primary Keys on Insert <---")
  val retrivePK = messages returning messages.map(_.id) += Message("Adrian", "its hot out here")
  logToConsole("PrimaryKey: " + exec(retrivePK))

  val messageReturningId = messages returning messages.map(_.id)

  logToConsole("PrimaryKey after insert: " + exec(messageReturningId +=Message("Kamoche", "ok, you can come inside if you want")))

  logToConsole("---> 3.1.4 Retrieving Rows on Insert <---")
  val messageReturingRows = messages returning messages.map(_.id) into { (message,id) =>
    message.copy(id = id)
  }

  val insert: DBIO[Message]= messageReturingRows += Message("Adrian", "No, am not done yet.")

  logToConsole("Retrive message after insert: " + exec(insert))

  logToConsole("---> 3.1.5 Inserting Specific Columns <---")

  logToConsole("Sql Statement -> "  + messages.map(_.sender).insertStatement)

  logToConsole("Inserting row: exec(messages.map(_.sender) += \"Kamoche\")")

  logToConsole("---> 3.1.6 Inserting Multiple Rows <---")

  val testMessages = Seq(
    Message("Dave", "Hello, HAL. Do you read me, HAL?"),
    Message("HAL", "Affirmative, Dave. I read you."),
    Message("Dave", "Open the pod bay doors, HAL."),
    Message("HAL", "I'm sorry, Dave. I'm afraid I can't do that.")
  )

  val bulkInsert = exec(messages ++= testMessages) match {
    case Some(count) => count
    case _ => None
  }

  logToConsole("Bulk Insert count: " + bulkInsert)

  logToConsole("---> 3.1.7 More Control over Inserts <---")

  val data = Query(("Stanley", "Cut!"))

  val exists =
    messages.
      filter(m => m.sender === "Stanley" && m.content === "Cut!")
      .exists

  val selectExpression = data.filterNot(_ => exists)

  val action =
    messages.
      map(m => m.sender -> m.content).
      forceInsertQuery(selectExpression)

  logToConsole("Force Inserct query count: " + exec(action))


  logToConsole("---> 3.2 Deleting Rows <---")

  val deleteHal= messages.filter(_.sender === "HAL").delete

  logToConsole("Delete sql statement -> " + deleteHal.statements.head)

  logToConsole("HAL records deleted: " + exec(deleteHal))

  logToConsole("---> 3.3. UPDATING ROWS <---")
  logToConsole("Populating data after deleting them")
  exec(messages.delete >> (messages ++=testMessages) >> messages.result).foreach(println)

  logToConsole("3.3.1 Updating a Single Field")

  val updateQuery=messages.filter(_.sender === "HAL").map(_.sender)
  logToConsole(updateQuery.updateStatement)
  logToConsole("Row affected by the update: " + exec(updateQuery.update("HAL 9000")))
  logToConsole("Updated records \n")
  exec(messages.result).foreach(println)

  val multipleColumnUpdate= messages.filter(_.id === 15L).map( m => (m.sender,m.content))
  logToConsole("Sql statement for multiple column update ->" + multipleColumnUpdate.updateStatement)
  logToConsole("Update message with id 15 -> " + exec(multipleColumnUpdate.update("HAL 9000", "Damn it")))

  logToConsole("Select message with id 15\n")
  exec(messages.filter(_.id === 15L).result).foreach(println)

  case class NameText(name: String, text: String)

  val newValue=NameText("Dave", "That's how its done")

  val updateDave= messages.filter(_.id === 14L).map(m => (m.sender,m.content).mapTo[NameText]).update(newValue)

  logToConsole("Update Dave record: " + exec(updateDave) + "\n")
  exec(messages.result).foreach(println)


  logToConsole("--------------------> 3.5 Exercises <-------------------------")
  logToConsole("---> 3.5.1 Get to the Specifics <---")

  val updateQuery1=messages.map(m => (m.sender,m.content))

  logToConsole("Inserting Specific Columns: " + exec(updateQuery1 +=("Kamoche", "Feeling sleepy, need coffee ")))

  exec(messages.result).foreach(println)


  val conversation = List(
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

  logToConsole("---> 3.5.2 Bulk All the Inserts <---")

  val populateDBReturnMessageWithIds = messages returning messages.map(_.id) into {  (message,id ) =>
    message.copy(id = id)
  }

  logToConsole("Populate DB return message with ids: \n")
  exec(populateDBReturnMessageWithIds ++= conversation).foreach(println)

  logToConsole("---> 3.5.3 No Apologies <---")
  val deleteMessageWithSorry = messages.filter(_.content like "%sorry%").delete

  logToConsole("Delete messages that contain sorry: " + exec(deleteMessageWithSorry))


  logToConsole("---> 3.5.4 Update Using a For Comprehension <---")

  val rebootLoop = messages.
    filter(_.sender === "HAL").
    map(msg => (msg.sender, msg.content)).
    update(("HAL 9000", "Rebooting, please wait..."))

  val rebootWithForLoop = for {
    m <- messages if  m.sender === "HAL"
  } yield (m.sender,m.content)

  val rebootAction=rebootWithForLoop.update("HAL 9000", "Rebooting, please wait.....")

  logToConsole("Rebooting ---> " + exec(rebootAction))


  logToConsole("---> 3.5.5 Selective Memory <---")

  val firstTwoBobMessages= messages.filter(_.sender === "Bob").sortBy(_.id.asc).take(2)

  logToConsole("Query the first two Bobs messages: " + exec(firstTwoBobMessages.result))


//  val getBobsId=firstTwoBobMessages.
  val deleteFirstTwoBobMessages=messages.filter(_.id in firstTwoBobMessages.map(_.id)).delete
  logToConsole("Delete the first two Bobs messages: " + exec(deleteFirstTwoBobMessages))

  logToConsole("Records after deleting Bobs \n")
  exec(messages.result).foreach(println)

}
