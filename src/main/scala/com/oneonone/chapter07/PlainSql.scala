package com.oneonone.chapter07

import java.time.LocalDateTime

import slick.jdbc.JdbcProfile
import slick.lifted.MappedTo

import scala.concurrent.Await

object PlainSql extends App {

  import ChatSchema._
  import scala.concurrent.duration._
  import scala.concurrent.ExecutionContext.Implicits.global

  val schema = new Schema(slick.jdbc.H2Profile)

  import schema._, profile.api._

  val db = Database.forConfig("chapter07")

  def exec[T](action: DBIO[T]): T = Await.result(db.run(action), 2 seconds)

  try {
    val roomSetUp = DBIO.seq(
      ddl.create,
      rooms ++= Seq(Room("Server room"), Room("Mez room"), Room("Security room"),  Room("Mez room"))
    )

    exec(roomSetUp)

    def logToConsole(msg: Any): Unit = {
      println(s"\n${msg}\n")
    }

    logToConsole("-------------> Selects <-------------------")

    val roomIds = sql""" SELECT "id" FROM "room" """.as[Long]

    logToConsole("Rooms Ids " + exec(roomIds))

    val roomsInfo = sql""" SELECT "id","title" from "room" """.as[(Long, String)]

    logToConsole("Room Info: " + exec(roomsInfo))

    val roomName = "Mez room"
    val podRoomAction = sql"""
                              select
                                "id", "title"
                              from
                                "room"
                              where
                                "title" = $roomName """.as[(Long, String)].headOption

    logToConsole("Find mez room: "+ exec(podRoomAction))

    import slick.jdbc.GetResult

    implicit val GetLocalDateTime=GetResult(r => r.nextTimestamp().toLocalDateTime)
    implicit val GetLocalDateTimeOption=GetResult(r => r.nextTimestampOption().map( rr => r.nextTimestamp().toLocalDateTime))

    val timestamps = exec {
      sql""" select "created" from "message" """.as[LocalDateTime]
    }
    println(s"Timestamps: $timestamps")

//    implicit val GetMessage = GetResult(r =>
//      Message(
//        sender = r.<<,
//        content = r.<<,
//        created = r.<<,
//        updated = r.<<?,
//        id  = r.<<
//      )
//    )
//
//    val action: DBIO[Seq[Message]] =
//      sql""" select * from "message" """.as[Message]

    val action =
      sqlu"""UPDATE "message" SET "content" = CONCAT("content", '!')"""

  } finally db.close()
}
