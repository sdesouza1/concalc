import akka.actor.{ ActorRef, ActorSystem, Props, Actor, Inbox,ActorLogging }
import scala.concurrent.duration._
import java.util.Scanner
import java.io.File
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

case class number(num:Int)
case class operation(op:Char)
case class Equation(num1:Int,operator:Char,num2:Int)
case class Result(num1:Int,operator:Char,num2:Int,result:Long)
case class file(path:String,n:Int)
case class requestFile()
case class beginDist(list:ArrayBuffer[String])
case class endFile()
case class currentFile(n:String)



class Evaluator extends Actor with ActorLogging{
    
    def receive ={
        case Equation(n1,o,n2)=>{
         // log.info("got equation from "+sender.path.toString)
          o match{
            case '+'=>{
              sender!Result(n1,'+',n2,n1+n2)
              context.stop(self)
            }
            case '-'=>{
              sender!Result(n1,'-',n2,n1-n2)
              context.stop(self)
            }
            case '*'=>{
              sender!Result(n1,'*',n2,n1*n2)
              context.stop(self)
            }
            case '%'=>{
              sender!Result(n1,'%',n2,n1%n2)
              context.stop(self)
            }
          }
        }
    }
}

class Receptionist extends Actor with ActorLogging{
    //val system = ActorSystem("calculator")
    var count=0
    var num1=0
    var num2=0
    var file=""
    var operator='+'
    var flag1=false
    var flag2=false
    var opflag=false
    val valid="+-%*"
    def receive={
      	case currentFile(n)=>{
      		file=n
      	}
        case number(n) =>{
         // log.info("got number "+n.toString+" from "+sender.path.toString)
            if(!flag1) {
                num1=n
                flag1=true
            }
            else if(!flag2) {
                num2=n
                flag2=true
            }
            if(flag1&&flag2&&opflag) {
                context.actorOf(Props[Evaluator],"evaluator"+count.toString())!Equation(num1,operator,num2)
                count+=1
                flag1=false
                flag2=false
                opflag=false
            }
        }
        case operation(op)=>{
          //log.info("got operator from "+sender.path.toString)
            if((!opflag)&&(valid contains op)) {
                operator=op
                opflag=true
                if(flag1&&flag2&&opflag){
                  context.actorOf(Props[Evaluator],"evaluator"+count.toString())!Equation(num1,operator,num2)
                  count+=1
                  flag1=false
                  flag2=false
                  opflag=false
                } 
            }
        }
        case Result(one,op,two,result)=>println(new java.util.Date+self.path.toString+" Result of "+one.toString()+op+two.toString()+"="+result.toString() +" from "+sender.path.toString+" while reading "+file)
        
    }
}

class Reader extends Actor with ActorLogging{
  val system=ActorSystem("calc")
   def isNumeric(in:String):Boolean=in.forall(_.isDigit)
   val valid="+-*%"
   
  def receive={
    
    case file(fp,n)=>{
      val recep=system.actorOf(Props[Receptionist],"recep"+n.toString)
      
      recep!currentFile(fp)
      //log.info("Got "+fp+" from "+sender.toString)
      val sc:Scanner=new Scanner(new File(fp))//.useDelimiter(" ")
      while(sc.hasNext){
    	  var x=sc.next
    	  if( isNumeric(x.trim)) recep!number(x.trim.toInt)
    	  else if(x.trim.length==1&&(valid contains x.trim.charAt(0))) recep!operation(x.trim.charAt(0))
      }
     // log.info(self.path.toString+" has run out of file to read. Requesting More.")
     sc.close
      sender!endFile
      context.stop(self)
    }
  }
}

class Distributor extends Actor with ActorLogging{
 
  var files=ArrayBuffer[String]("","")
  val rnd=new Random()
  def receive={
    case beginDist(fs)=>{
     log.info("received begin msg")
      files=fs
      for(i<-0 to 10){
        log.info("making reader "+i)
        context.actorOf(Props[Reader],"reader"+i.toString)!file(files(rnd.nextInt(files.length)),i)
      }
      
    }
    case endFile=>{
      var y=rnd.nextInt(10)
      context.actorOf(Props[Reader],sender.path.name+y.toString)!file(files(rnd.nextInt(files.length)),y)
    }
    
  }
}

object HelloAkkaScala extends App {

  val system = ActorSystem("calculator")

  
  
  val inbox = Inbox.create(system)
 
// val files=ArrayBuffer[String]("input.txt")
  val files=ArrayBuffer[String]("input1.txt","input2.txt","input3.txt","input4.txt","input5.txt","input6.txt","input7.txt","input8.txt","input9.txt","input10.txt","input11.txt","input12.txt","input13.txt","input14.txt","input15.txt","input16.txt","input17.txt","input18.txt","input19.txt","input20.txt","input21.txt","input22.txt","input23.txt","input24.txt","input25.txt","input26.txt","input27.txt","input28.txt","input29.txt","input30.txt","input31.txt","input32.txt","input33.txt","input34.txt","input35.txt","input36.txt","input37.txt","input38.txt","input39.txt","input40.txt","input41.txt","input42.txt","input43.txt","input44.txt","input45.txt","input46.txt","input47.txt","input48.txt","input49.txt","input50.txt","input51.txt","input52.txt","input53.txt","input54.txt","input55.txt","input56.txt","input57.txt","input58.txt","input59.txt","input60.txt","input61.txt","input62.txt","input63.txt","input64.txt","input65.txt","input66.txt","input67.txt","input68.txt","input69.txt","input70.txt","input71.txt","input72.txt","input73.txt","input74.txt","input75.txt","input76.txt","input77.txt","input78.txt","input79.txt","input80.txt","input81.txt","input82.txt","input83.txt","input84.txt","input85.txt","input86.txt","input87.txt","input88.txt","input89.txt","input90.txt","input91.txt","input92.txt","input93.txt","input94.txt","input95.txt","input96.txt","input97.txt","input98.txt","input99.txt","input100.txt")
  val dist=system.actorOf(Props[Distributor],"dist")
  
  inbox.send(dist,beginDist(files))

 
 
}

