package com.oneonone.chapter01

import slick.dbio.DBIO
import slick.jdbc.H2Profile.api._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._


trait Helper{
  val db=Database.forConfig("chapter01")

  def exec[T](action: DBIO[T]): T = Await.result(db.run(action), 3 seconds)
}
object Main extends App with Helper {



  case class Message(sender: String, content: String, id: Long = 0)

  final class MessageTable(tag: Tag) extends Table[Message](tag, "MESSAGE") {

    val id = column[Long]("ID", O.PrimaryKey, O.AutoInc)
    val sender = column[String]("SENDER")
    val content = column[String]("CONTENT")

    override def * = (sender, content, id).mapTo[Message]
  }

  lazy val messages= TableQuery[MessageTable]


  val action:DBIO[Unit] = messages.schema.create


  val result: Unit= exec(action)

    def freshDataEntry = Seq(
      Message("KAMOCHE","I am the greatest of all time"),
      Message("Sharon","The most beautiful lady"),
      Message("Adrian", "Cutest in the world, not to mention how smart he is")
    )


  val insert:DBIO[Option[Int]] = messages ++= freshDataEntry

  val rowCount:Option[Int]= exec(insert)




  val statement=messages.result.statements.mkString


  //subset of the messages

  val shazMessageStatement=messages.filter(_.sender === "Sharon").result.statements.mkString
  val shazSay1=messages.filter(_.sender === "Sharon")

  val shazMessages: Seq[Message] = exec(shazSay1.result)


  val shazMessageContent= shazSay1.map(_.id)

  val result1= exec(shazMessageContent.result)

  val shazSay = for {
    message <- messages if message.sender === "Sharon"
  } yield message

  exec(shazSay.result)


  //combining actions

  val action1: DBIO[Seq[Message]] =
    messages.schema.create andThen
    (messages ++= freshDataEntry) andThen
    shazSay.result

  //get fanky

  val samAction1: DBIO[Seq[Message]] =
      messages.schema.create >>
      (messages ++= freshDataEntry) >>
      shazSay.result

  //  val action: DBIO[Unit]=
}
