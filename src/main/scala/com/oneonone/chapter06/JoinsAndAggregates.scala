package com.oneonone.chapter06
import slick.jdbc.JdbcProfile

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object JoinsAndAggregates extends App{


  trait Profile{
    val profile: JdbcProfile
  }

  trait Tables{
    this: Profile =>
    import profile.api._

    case class User(name: String, id: Long = 0L)
    class UserTable(tag: Tag) extends Table[User](tag,"user"){
      def id=column[Long]("id", O.PrimaryKey, O.AutoInc)
      def name=column[String]("name")

      def * =(name,id).mapTo[User]
    }

    lazy val users= TableQuery[UserTable]
    lazy val insertUser= users returning users.map(_.id)


    case class Message(senderId: Long, content: String, id: Long = 0L)

    class MessageTable(tag: Tag) extends Table[Message](tag, "message"){
      def id=column[Long]("id",O.PrimaryKey, O.AutoInc)
      def content= column[String]("content")
      def senderId= column[Long]("senderId")

      def sender= foreignKey("sender_fk", senderId, users)(_.id, onDelete = ForeignKeyAction.Cascade)

      def * = (senderId,content,id).mapTo[Message]

    }

    lazy val messages=TableQuery[MessageTable]
    lazy val insertMessage= messages returning messages.map(_.id)
  }


  class Schema(val profile: JdbcProfile) extends Tables with Profile

  val schema= new Schema(slick.jdbc.H2Profile)

  import schema._ , profile.api._

  val db=Database.forConfig("chapter06")

  def exec[T](action: DBIO[T]): T = Await.result(db.run(action), 2 seconds)

  def freshTestData(daveId: Long, halId: Long) = Seq(
    Message(daveId, "Hello, HAL. Do you read me, HAL?"),
    Message(halId,"Affirmative, Dave. I read you."),
    Message(daveId, "Open the pod bay doors, HAL."),
    Message(halId, "I'm sorry, Dave. I'm afraid I can't do that.")
  )

  val setup = for {
    _ <- (users.schema ++ messages.schema).create
    daveId <- insertUser += User("Dave")
    halId <- insertUser += User("HAL")
    rowsAdded <- messages ++= freshTestData(daveId, halId)
  } yield rowsAdded

  val join = for {
    msg <- messages
    usr <- msg.sender
  } yield (usr.name , msg.content)

  val join1= messages flatMap { msg =>
    msg.sender.map{ usr =>
      (usr.name, msg.content)
    }
  }

  val join2 = for {
    msg <- messages
    usr <- users if usr.id === msg.senderId
  } yield (usr.name, msg.content)

  val join3: Query[(MessageTable, UserTable), (Message, User), Seq] =
    messages join users on (_.senderId === _.id)

  val join4: Query[(MessageTable, UserTable), (Message, User), Seq] =
    messages join users on ( (m: MessageTable, u: UserTable) =>
      m.senderId === u.id )

  val join5: Query[(MessageTable, UserTable), (Message, User), Seq] =
    messages join users on {
      case (m, u) =>
      m.senderId === u.id
    }

  val action: DBIO[Seq[(Message, User)]] =join3.result

  exec(action).foreach(println)


//  exec(join.result)
}
