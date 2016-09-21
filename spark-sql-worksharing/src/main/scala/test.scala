/**
  * Created by ntkhoa on 21/07/16.
  */

import javax.mail.internet.InternetAddress

import courier._, Defaults._

import scala.concurrent.Await

object test {
  def main(args: Array[String]) {
    println((1e-2))
//    val mailer = Mailer("smtp.gmail.com", 25)
//      .auth(true)
//      .as("khoa.mailer@gmail.com", "abcxyz1234")
//      .startTtls(true)()
//
//    val f =mailer(Envelope.from(new InternetAddress("khoa.mailer@gmail.com"))
//      .to(new InternetAddress("ngtrkhoa@gmail.com"))
//      .subject("Job done")
//      .content(Text("pls check ui")))
//
//    val x = Await.ready(f, scala.concurrent.duration.Duration.Inf).value.get
//    println(x)
  }

}
