package com.oneonone.chapter05

import slick.jdbc.JdbcProfile

import scala.concurrent.Await

import scala.concurrent.duration._

import scala.concurrent.ExecutionContext.Implicits.global

object NullExample extends App{

  trait Profile{
    val profile: JdbcProfile
  }

  trait Tables{
    this: Profile =>

    import profile.api._

    final case class User(name: String, email: Option[String] = None, id: Long = 0L )

    final case class UserTable(tag: Tag) extends Table[User](tag, "USER"){
      val id=column[Long]("ID", O.PrimaryKey, O.AutoInc)
      val name=column[String]("NAME")
      val email=column[Option[String]]("EMAIL")

      def * = (name,email,id).mapTo[User]
    }

    lazy val users=TableQuery[UserTable]
    lazy val insertUser=users returning users.map(_.id)
  }

  class Schema(val profile: JdbcProfile) extends Tables with Profile

  val schema=new Schema(slick.jdbc.H2Profile)

  import schema._, profile.api._

  val db=Database.forConfig("chapter05")

  def exec[T](action: DBIO[T]): T = Await.result(db.run(action), 2.seconds)

  val program= for {
    _ <- users.schema.create
    kamocheId <- insertUser +=User("Kamoche", Some("kamoche.codemaster@gmail.com"))
    sharonId <- insertUser +=User("Sharon", Some("shaz.python@gmail.com"))
    adrianId <- insertUser +=User("Adrian")
    family <- users.result
  } yield family


  println("\n ------------> User with optional email addresses <-------------------")
  exec(program).foreach(println)

  println("\n ------------> Family with null email first < ------------------------")

  exec(users.sortBy(_.email.asc.nullsFirst).result).foreach(println)

}
