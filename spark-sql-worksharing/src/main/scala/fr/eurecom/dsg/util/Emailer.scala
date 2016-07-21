package fr.eurecom.dsg.util
import javax.mail.internet.InternetAddress
import courier._, Defaults._
import scala.concurrent.Await

object Emailer {
  val senderAddr = "khoa.mailer@gmail.com"
  val password = "abcxyz1234"
  val toAddr = "ngtrkhoa@gmail.com"

  def sendMessage(subject:String, msg:String): Unit ={
    val mailer = Mailer("smtp.gmail.com", 587)
      .auth(true)
      .as(senderAddr, password)
      .startTtls(true)()

    val f =mailer(Envelope.from(new InternetAddress("khoa.mailer@gmail.com"))
      .to(new InternetAddress(toAddr))
      .subject(subject)
      .content(Text(msg)))

    Await.ready(f, scala.concurrent.duration.Duration.Inf).value.get
  }

}
