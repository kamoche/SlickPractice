package com.oneonone.chapter07

import java.sql.Timestamp
import java.time.LocalDateTime

import slick.jdbc.JdbcProfile
import slick.lifted.MappedTo



object ChatSchema {

  trait  Profile{
    val profile: JdbcProfile
  }

  case class Id[A](value: Long) extends AnyVal with MappedTo[Long]

  object PKs{
    import slick.lifted.MappedTo
    case class RoomPK(value: Long) extends AnyVal with MappedTo[Long]
  }

  trait Tables {
    this: Profile =>

    import profile.api._

    import PKs._

    implicit  val convertLocalDTToTimeStamp = MappedColumnType.base[LocalDateTime, Timestamp](
      lt => Timestamp.valueOf(lt),
      ts => ts.toLocalDateTime
    )
    case class Room(title: String, id: Id[RoomTable] = Id(0L))

    class RoomTable(tag: Tag) extends Table[Room](tag, "room") {
      def id    = column[Id[RoomTable]]("id", O.PrimaryKey, O.AutoInc)
      def title = column[String]("title")

      def *     = (title, id).mapTo[Room]
    }

    lazy val rooms = TableQuery[RoomTable]
    lazy val insertRoom = rooms returning rooms.map(_.id)

    case class Message(
                        sender: String,
                        content: String,
                        created: LocalDateTime,
                        updated: Option[LocalDateTime],
                        id: Id[MessageTale] = Id(0L))

    class MessageTale(tag: Tag) extends Table[Message](tag, "message"){
      def id      = column[Id[MessageTale]]("id", O.PrimaryKey, O.AutoInc)
      def sender  = column[String]("sender")
      def content = column[String]("content")
      def created = column[LocalDateTime]("created")
      def updated = column[Option[LocalDateTime]]("updated")

      def *       = (sender,content,created,updated,id).mapTo[Message]
    }

    lazy val messages = TableQuery[MessageTale]
    lazy val insertMessage = messages returning messages.map(_.id)

    val ddl = rooms.schema ++ messages.schema


  }

  class Schema(val profile: JdbcProfile) extends Tables with Profile
}
