package com.oneonone.chapter05

import slick.jdbc.JdbcProfile

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object PrimaryKeys extends App{

  trait Profile{
    val profile: JdbcProfile
  }

  trait Tables{

    this: Profile =>

    import profile.api._

    case class User(id: Option[Long], name: String, email: Option[String] = None)

    class UserTable(tag: Tag) extends Table[User](tag, "USER"){

      def id =column[Long]("ID", O.PrimaryKey, O.AutoInc)
      def name=column[String]("NAME")
      def email=column[Option[String]]("EMAIL")

      def * = (id.?, name, email).mapTo[User]
    }

    lazy val users=TableQuery[UserTable]
    lazy val insertUser= users returning users.map(_.id)

    case class Room(title: String, id: Long = 0L)

    class RoomTable(tag: Tag) extends Table[Room](tag, "ROOM"){
      def id=column[Long]("ID", O.PrimaryKey, O.AutoInc)
      def title=column[String]("TITLE")

      def * = (title,id).mapTo[Room]
    }

    lazy val rooms=TableQuery[RoomTable]
    lazy val insertRoom= rooms returning rooms.map(_.id)

    case class Occupant(roomId: Long, userId: Long)

    class OccupantTable(tag: Tag) extends Table[Occupant](tag, "OCCUPANT"){
      def roomId=column[Long]("ROOMID")
      def userId=column[Long]("USERID")
      def pk=primaryKey("room_user_pk",(roomId,userId))
      def * = (roomId,userId).mapTo[Occupant]
    }

    lazy val occupants=TableQuery[OccupantTable]

    val ddl = users.schema ++ rooms.schema ++ occupants.schema
  }

  class Schema(val profile: JdbcProfile) extends Tables with Profile

  val schema= new Schema(slick.jdbc.H2Profile)

  import schema._, profile.api._


  def exec[T](action: DBIO[T])=Await.result(db.run(action), 2.seconds)

  val db=Database.forConfig("chapter05")

  val init = for {
    _         <- ddl.create
    kamocheId    <- insertUser += User(None, "Kamoche", Some("kamoche@example.org"))
    adrianId     <- insertUser += User(None, "Adrian")
    sharonId     <- insertUser += User(None, "Sharon", Some("sharon@example.org"))
    scalaLabId <- insertRoom += Room("Scala lab")
    _         <- occupants += Occupant(scalaLabId, kamocheId)
  } yield ()

  exec(init)

  println("\nUsers database contains:")
  exec(users.result).foreach { println}

  println("\nOccupation is:")
  exec(occupants.result).foreach { println}
}
