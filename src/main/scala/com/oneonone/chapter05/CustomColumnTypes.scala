package com.oneonone.chapter05

import java.sql.Timestamp
import java.time.LocalDateTime

import slick.jdbc.JdbcProfile
import slick.lifted.MappedTo

import scala.concurrent.Await
import scala.concurrent.duration._

import scala.concurrent.ExecutionContext.Implicits.global

object CustomColumnTypes extends App{

  trait Profile{
    val profile: JdbcProfile
  }


  object PKs{
    import slick.lifted.MappedTo
    case class MessagePK(value: Long) extends AnyVal with MappedTo[Long]
    case class UserPK(value: Long) extends AnyVal with MappedTo[Long]
  }

  trait Tables {
    this: Profile =>
    import profile.api._
    import PKs._

    implicit val jodaDateTimeType = MappedColumnType.base[LocalDateTime,Timestamp ](
      dt => Timestamp.valueOf(dt),
      ts => ts.toLocalDateTime
    )


    case class User(name: String, id:  UserPK = UserPK(0L))

    class UserTable(tag: Tag) extends Table[User](tag,"user"){
      def id=column[UserPK]("id", O.PrimaryKey, O.AutoInc)
      def name=column[String]("name")

      def * = (name,id).mapTo[User]
    }

    lazy val users=TableQuery[UserTable]
    lazy val insertUser= users returning users.map(_.id)

    //124

    case class Message(senderId: UserPK,
                       content: String,
                       timestamp: LocalDateTime,
                       id: MessagePK = MessagePK(0L))

    class MessageTable(tag: Tag) extends Table[Message](tag, "message1"){
      def id= column[MessagePK]("id", O.PrimaryKey, O.AutoInc)
      def content= column[String]("content")
      def timestamp=column[LocalDateTime]("timestamp")
      def senderId=column[UserPK]("senderId")

      def * = (senderId,content,timestamp,id).mapTo[Message]

      def sender=foreignKey("sender_pk", senderId, users)(_.id, onDelete = ForeignKeyAction.Cascade)

    }

    lazy val messages=TableQuery[MessageTable]
    lazy val insertMessage=messages returning messages.map(_.id)

    lazy val ddl= users.schema ++ messages.schema

  }

  class Schema(val profile: JdbcProfile) extends Tables with Profile

  val schema= new Schema(slick.jdbc.H2Profile)

  import schema._, profile.api._

  def exec[T](action: DBIO[T]): T =  Await.result(db.run(action), 2 seconds)

  val db = Database.forConfig("chapter05")

  val conversation= for {
    _ <- users.schema.create
    _ <- messages.schema.create
    userId <- insertUser += User("Kamoche")
    adrianId <- insertUser +=User("Adrian")
    count <- messages ++=Seq(
      Message(userId, "Hello Adrian", LocalDateTime.now()),
      Message(adrianId, "Am fine dad", LocalDateTime.now()),
      Message(userId, "Where have you been?", LocalDateTime.now()),
      Message(adrianId, "Went for mountain climbing", LocalDateTime.now())

    )

  } yield count






//  println(exec(join))

  println(exec(conversation))

  exec(messages.result).foreach(println)




}
