package com.oneonone.chapter06

import slick.jdbc.JdbcProfile
import slick.lifted.MappedTo

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global


object ChatSchema{


  trait Profile{
    val profile: JdbcProfile
  }

  object PKs{
    import slick.lifted.MappedTo
    case class UserPK(value: Long) extends AnyVal with MappedTo[Long]
    case class RoomPK(value: Long) extends AnyVal with MappedTo[Long]
    case class MessagePK(value: Long) extends AnyVal with MappedTo[Long]
  }

  trait Tables{

    this: Profile =>

    import profile.api._
    import PKs._

    case class User(name: String, id: UserPK = UserPK(0L))

    class UserTable(tag: Tag) extends Table[User](tag,"user"){
      def  id=column[UserPK]("id",O.PrimaryKey, O.AutoInc)
      def name=column[String]("name")

      def * =(name,id).mapTo[User]
    }
    lazy val users=TableQuery[UserTable]
    lazy val insertUser= users returning users.map(_.id)

    case class Room(title: String, id: RoomPK = RoomPK(0L))

    class RoomTable(tag: Tag) extends Table[Room](tag, "room"){

      def id=column[RoomPK]("id", O.PrimaryKey, O.AutoInc)
      def title=column[String]("title")

      def * =(title,id).mapTo[Room]
    }

    lazy val rooms=TableQuery[RoomTable]
    lazy val insertRoom= rooms returning rooms.map(_.id)

    case class Message(senderId: UserPK, content: String, roomId: Option[RoomPK] = None, id: MessagePK = MessagePK(0L))

    class MessageTable(tag: Tag) extends Table[Message](tag, "message"){
      def id=column[MessagePK]("id", O.PrimaryKey,O.AutoInc)
      def content=column[String]("content")
      def senderId=column[UserPK]("sender")
      def roomId= column[Option[RoomPK]]("room")

      def sender= foreignKey("sender_fk", senderId, users)(_.id, onDelete = ForeignKeyAction.Cascade)
      def room = foreignKey("room_fk", roomId, rooms)(_.id)

      def * = (senderId, content, roomId, id).mapTo[Message]
    }

    lazy val messages=TableQuery[MessageTable]
    lazy val insertMessage= messages returning messages.map(_.id)

    val ddl= users.schema ++ rooms.schema ++ messages.schema

  }

  class Schema(val profile: JdbcProfile) extends Tables with Profile


}

object LongerJoins extends App{

  import ChatSchema._

  val schema=new Schema(slick.jdbc.H2Profile)

  import schema._, profile.api._
  import PKs._

  val db=Database.forConfig("chapter06")
  def exec[T](action: DBIO[T]): T = Await.result(db.run(action), 3 seconds)



  val initData= for {
    _ <- ddl.create
    airLockId <- insertRoom += Room("Air Lock")
    kamocheId <- insertUser += User("Kamoche")
    adrianId <- insertUser += User("Adrian")
    // Half the messages will be in the air lock room...
    _ <- insertMessage += Message(kamocheId, "Hello, HAL. Do you read me, HAL?",  Some(airLockId))
    _ <- insertMessage += Message(adrianId, "Affirmative, Dave. I read you.",  Some(airLockId))
    // ...and half will not be in room:
    _ <- insertMessage += Message(kamocheId, "Open the pod bay doors, HAL.")
    _ <- insertMessage += Message(adrianId, "I'm sorry, Dave. I'm afraid I can't do  that.")
    msg <- messages.result
  } yield (msg)

  exec(initData).foreach(println)

  val usersAndRooms=
    messages
      .join(users).on(_.senderId === _.id)
      .join(rooms).on{case ((msg,user),room) => msg.roomId === room.id}

  val usersAndRooms1=
    messages
      .join(users).on(_.senderId === _.id)
      .join(rooms).on{_._1.roomId === _.id}

  def logToConsole(msg: Any): Unit ={
    println(s"\n${msg}\n")
  }


  logToConsole("------------------> Inner Join <----------------------")
  exec(usersAndRooms.result).foreach(println)

  logToConsole("-----------------> Mapping Joins <-------------------")

  val mappedJoins=usersAndRooms.map{
    case ((msg,user),room) => (msg.content, user.name, room.title)
  }

  exec(mappedJoins.result).foreach(println)

  logToConsole("-------------------> Filter with Joins <----------------")

  val airLockMsg= usersAndRooms.filter{ case (_, room) => room.title === "Air Lock"  }

  exec(airLockMsg.result).foreach(println)

  //145

  logToConsole(ddl.createStatements.mkString("\n"))
  logToConsole("-----------------> Left Join <-------------------------")

  val leftJoin=messages.joinLeft(rooms).on(_.roomId === _.id).map{ case (msg, room) => (msg.content, room.map( _.title))}

  exec(leftJoin.result).foreach(println)

  val rightJoin= for {
    (msg,room) <- messages.joinRight(rooms).on(_.roomId === _.id)
  } yield (room.title, msg.map(_.content))

  exec((insertRoom += Room("Server room")))



  exec(rightJoin.result).foreach(println)

  logToConsole("-----------------> Full Outer Join <---------------------")

  val fullJoin = for {
    (room,msg) <- rooms joinFull messages on(_.id === _.roomId)
  } yield (room.map(_.title), msg.map(_.content))


  exec(fullJoin.result).foreach(println)

  logToConsole("-----------------> AGGREGATION <-----------------------")

  val numRows: DBIO[Int] = messages.length.result
  val numDifferentSenders: DBIO[Int] = messages.map(_.senderId).distinct.length.result
  val firstSent: DBIO[Option[MessagePK]] =  messages.map(_.id).min.result

  println(exec(numRows))
  println(exec(numDifferentSenders))
  println(exec(firstSent))

  logToConsole("-----------------> Grouping <---------------------------")

  val msgPerUser: DBIO[Seq[(UserPK, Int)]] =
    messages.groupBy(_.senderId)
      .map { case (senderId,msg) => senderId -> msg.length}
      .result

  println(exec(msgPerUser))

  val msgsPerUser1 =
    messages.join(users).on(_.senderId === _.id)
      .groupBy { case (msg, user)  => user.name }
      .map { case (name, group) => name -> group.length }.
    result

  exec(msgsPerUser1).foreach(println)


  val stats =
    messages.join(users).on(_.senderId === _.id).
      groupBy { case (msg, user) => user.name }.
      map
      {
        case (name, group) =>
          (name, group.length, group.map{ case (msg, user) => msg.id}.min)
      }


  import scala.language.higherKinds

  def idOf[S[_]](group: Query[(MessageTable,UserTable), (Message,User), S]) =
    group.map { case (msg, user) => msg.id }

  val nicerStats =
    messages.join(users).on(_.senderId === _.id).
      groupBy { case (msg, user) => user.name }.
      map { case (name, group) => (name, group.length, idOf(group).min) }

  messages.
    filter(_.content like "%read%").
    groupBy(_ => true).
    map {
      case (_, msgs) => (msgs.map(_.id).min, msgs.map(_.id).max)
    }

  val addFrank = for {
    kitchenId <- insertRoom += Room("Kitchen")
    frankId   <- insertUser += User("Frank")
    rowsAdded <- messages ++= Seq(
      Message(frankId, "Hello?", Some(kitchenId)),
      Message(frankId, "Helloooo?", Some(kitchenId)),
      Message(frankId, "HELLO!?",  Some(kitchenId))
    )
  } yield rowsAdded

  exec(addFrank)

  val msgsPerRoomPerUser =
    rooms.
      join(messages).on(_.id === _.roomId).
      join(users).on{ case ((room,msg), user) => user.id === msg.senderId }.
      groupBy { case ((room,msg), user)  => (room.title, user.name) }.
      map { case ((room,user), group) => (room, user, group.length) }.
      sortBy { case (room, user, group)   => room }

  exec(msgsPerRoomPerUser.result).foreach(println)


  logToConsole("--------------------->  EXERCISE <--------------------------")


  val messageUser = for {
    msg <- messages
    usr <- msg.sender
  } yield (msg, usr)


  exec(messageUser.result).foreach(println)

  logToConsole("---> message content and user name <---")

  val messageUser1 = for {
    msg <- messages
    usr <- msg.sender
  } yield (msg.content, usr.name)


  exec(messageUser1.result).foreach(println)


  val exr2= messageUser1.sortBy{ case (content,name) => name.asc}


  logToConsole("---> Return name in order <---")
  exec(exr2.result).foreach(println)

  val exr3 =
    messages.join(users).on(_.senderId === _.id)
      .map { case (msg, usr) => (msg.content, usr.name) }
      .sortBy{ case (content,name) => name.asc }

  logToConsole("---> Applicative join <---")
  exec(exr3.result).foreach(println)



  def findByName(name: String): Query[Rep[Message], Message, Seq] =
    users.filter(_.name === name).
      join(messages).on(_.id === _.senderId).
      map{ case (usr,msg) => msg}

  logToConsole("---> FindByName <---")

  exec(findByName("Kamoche").result).foreach(println)

  logToConsole("---> Having Many Messages <---")

  val msgsPerUser2 =
    messages.join(users).on(_.senderId === _.id).
      groupBy { case (msg, user)  => user.name }.
      map { case (name, group) => name -> group.length }.
      filter {case (name, count) => count > 2 }

  exec(msgsPerUser2.result).foreach(println)





}
