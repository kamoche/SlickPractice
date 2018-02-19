package com.oneonone.chapter05


import java.sql.Timestamp
import java.time.LocalDateTime

import com.oneonone.chapter05.Exercise.PKs.UserPK
import slick.jdbc.JdbcProfile

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object Exercise extends App {

  trait Profile {
    val profile: JdbcProfile
  }

  object PKs{
    import slick.lifted.MappedTo
    case class UserPK(value: Long) extends AnyVal with MappedTo[Long]
    case class MessagePK(value: Long) extends AnyVal with MappedTo[Long]

  }

  object UserRole extends Enumeration{
    type UserRole = Value
    val Owner = Value("O")
    val Reqular = Value("R")
  }

  trait Tables {
    this: Profile =>
    import profile.api._
    import PKs._
    import UserRole._

    implicit val timeConversion=MappedColumnType.base[LocalDateTime, Timestamp](
      lt => Timestamp.valueOf(lt),
      ts => ts.toLocalDateTime
    )

//    implicit val userRole =MappedColumnType.base[UserRole, String](_.toString, UserRole.withName(_))
    implicit val userRoleMapper=MappedColumnType.base[UserRole, Int](
      _.id,
      v => UserRole.values.find(_.id == v) getOrElse Reqular
    )

    sealed trait Priority

    object HighPriority extends Priority
    object LowPriority extends Priority

    implicit val priorityType=MappedColumnType.base[Priority, String](
      flag => flag match {
        case HighPriority => "y"
        case LowPriority  => "l"
      },
      str => str match {
        case "h" | "H" | "+" | "high" => HighPriority
        case "N" | "n" | "-" | "low" => LowPriority
       }
    )

    case class User(name: String, email: Option[String], role: UserRole = Reqular, id: UserPK = UserPK(0L))

    class UserTable(tag: Tag) extends Table[User](tag, "user"){

      def id=column[UserPK]("id", O.PrimaryKey, O.AutoInc)
      def name=column[String]("name")
      def email=column[Option[String]]("email")
      def role = column[UserRole]("role", O.Length(1, false))

      def * = (name,email,role, id).mapTo[User]

    }

    lazy val users=TableQuery[UserTable]
    lazy val insertUser= users returning users.map(_.id)

    case class Message(senderId: UserPK, content: String, entryDateTime: LocalDateTime, id: MessagePK = MessagePK(0L))

    class MessageTable(tag: Tag) extends Table[Message](tag, "message"){
      def id=column[MessagePK]("id", O.PrimaryKey, O.AutoInc)
      def senderId=column[UserPK]("sender")
      def content=column[String]("content")
      def entryDateTime=column[LocalDateTime]("entryDateTime")

      def * =(senderId,content,entryDateTime,id).mapTo[Message]

      val sender= foreignKey("sender_fk", senderId, users)(_.id, onDelete = ForeignKeyAction.Cascade)
    }


    lazy val messages=TableQuery[MessageTable]
    lazy val insertMessage=messages returning messages.map(_.id)


    val ddl= users.schema ++ messages.schema
  }

  class Schema(val profile: JdbcProfile) extends Tables with Profile

  val schema= new Schema(slick.jdbc.H2Profile)

  import schema._ , profile.api._

  val db=Database.forConfig("chapter05")

  try {
    def exec[T](action: DBIO[T]): T = Await.result(db.run(action), 2.seconds)

    def logToConsole(log: Any): Unit ={
      println(s"\n ${log} \n")
    }

    val initialData = for {
      _ <- ddl.create
      sharonId <- insertUser += User("Sharon", Some("sharon@gmail.com"))
      adrianId <- insertUser += User("Adrian", None)
      mumSonConversation <- messages ++= Seq(
        Message(sharonId, "Hello baby boy", LocalDateTime.now),
        Message(adrianId, "Yes, am fine thank you", LocalDateTime.now),
        Message(sharonId, "Have you done your homework?", LocalDateTime.now),
        Message(adrianId, "Not yet, I was to start now", LocalDateTime.now)
      )
    } yield mumSonConversation

    println(exec(initialData))
    exec(users.result).foreach(println)
    exec(messages.result).foreach(println)

    logToConsole("-----> 5.6.1 Exercises Filtering Optional Columns <-----")

    def filterByEmail(email: Option[String]) = email match {
      case Some(value) => users.filter(_.email === value)
      case None => users
    }

    def filterByEmail1(email: Option[String]) = email.isEmpty match {
      case false => users.filter(_.email === email)
      case true => users
    }

    def filterByEmail2(email: Option[String]) = users.filter(u => u.email.isEmpty || u.email === email)


//    exec(
//      filterByEmail2(Some("sharon@gmail.com")).result
//    )foreach(println)

    exec(
      filterByEmail2(None).result
    ).foreach(println)


    val addMessage= messages +=Message(UserPK(200L),"User Id does not exist", LocalDateTime.now)

    logToConsole("-----> 5.6.3 Enforcement <-----")
    logToConsole(exec(addMessage.asTry))

    logToConsole("-----> 5.6.4 Mapping Enumerations <-----")




    }finally db.close()
}
