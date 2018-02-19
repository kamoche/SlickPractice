package com.oneonone.chapter05

import slick.jdbc.JdbcProfile


trait DatabaseModule{
  val profile: JdbcProfile
  import profile.api._

}

trait Profile{
  val profile: JdbcProfile
}
trait  DatabaseModule1{
  this: Profile =>
  // Write database code here
}
trait  DatabaseModule2{
  this: Profile =>
  // Write more database code here
}

//mix the module together

class DatabaseLayer(val profile: JdbcProfile) extends Profile with DatabaseModule1 with DatabaseModule2
class Main {

  val databaseLayer = new DatabaseModule {
    override val profile = slick.jdbc.H2Profile
  }

  val databaseLayer1 = new DatabaseLayer(slick.jdbc.H2Profile)



}
