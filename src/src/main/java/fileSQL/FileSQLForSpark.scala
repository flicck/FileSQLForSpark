package fileSQL



import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.streaming.StreamingContext

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object FileSQLForSpark {
  //每个sql刷新一次的outPaths
 private var outPaths = new ListBuffer[String]()
  //用于展示的outPathsShow
  private var outPathsShow = new ListBuffer[String]()

  //界限变量
  private var limit:Int=Int.MaxValue
  private var fileRegex = ".*"
  private var uLikeRegex = ""
  private var fileSizeUpBound:Long= Long.MaxValue
  private var fileSizeUpBoundCanEqual:Long = Long.MaxValue
  private var fileSizeLowBound:Long= -1L
  private var fileSizeLowBoundCanEqual:Long= -1L
  private var totalSizeUpBound:Long=Long.MaxValue
  private var totalSizeUpBoundCanEqual:Long=Long.MaxValue
  private var totalSizeLowBound:Long= -1L
  private var totalSizeLowBoundCanEqual:Long= -1L
  private var timeUpBound:Long=Long.MaxValue
  private var timeUpBoundCanEqual:Long=Long.MaxValue
  private var timeLowBound:Long= -1L
  private var timeLowBoundCanEqual:Long= -1L

  //全局工具变量
  private  var fileSize =0L
  private var aggregateTotalSize = 0L
  private var num = 0   //每个sql刷新一次
  private val fs: FileSystem = FileSystem.get(new Configuration())
  private var inputPath:String =""
  private var inputType = ""
  private var timeFormat: SimpleDateFormat = null

 // var inputTypeList = new ListBuffer[String]

  private def getFilesInner(ss:SparkSession,sql:String):DataFrame={
    import ss.implicits._
    //针对每一个sql重设限制
    outPaths = new ListBuffer[String]()
    limit=Int.MaxValue
    fileRegex = ".*"
    uLikeRegex = ""
    fileSizeUpBound= Long.MaxValue
    fileSizeUpBoundCanEqual= Long.MaxValue
    fileSizeLowBound= -1L
    fileSizeLowBoundCanEqual= -1L
    totalSizeUpBound=Long.MaxValue
    totalSizeUpBoundCanEqual=Long.MaxValue
    totalSizeLowBound= -1L
    totalSizeLowBoundCanEqual= -1L
    timeUpBound=Long.MaxValue
    timeUpBoundCanEqual=Long.MaxValue
    timeLowBound = -1L
    timeLowBoundCanEqual = -1L

    num = 0
    aggregateTotalSize = 0L

    val words: Array[String] = sql.split(" ")

    for(i<-0 until words.length){
      if(words(i).toLowerCase.equals("select")){
        //获取输入数据类型
        inputType = words(i+1).toLowerCase
      }
      if(words(i).toLowerCase.equals("from")){
        inputPath = words(i+1)
      }
      if(words(i).toLowerCase.equals("limit")){
        limit = words(i+1).toInt
      }
      if(words(i).toLowerCase.equals("filename")){
        if(words(i+1).toLowerCase.equals("rlike")){
          if(words(i+2).startsWith("'") && words(i+2).endsWith("'")){
            fileRegex = words(i+2).substring(1,words(i+2).length-1)
          }else{
            fileRegex = words(i+2)
          }
        }
        if(words(i+1).toLowerCase.equals("ulike")){
          if(words(i+2).startsWith("'") && words(i+2).endsWith("'")){
            uLikeRegex = words(i+2).substring(1,words(i+2).length-1)
          }else{
            uLikeRegex = words(i+2)
          }
        }
      }
      if(words(i).toLowerCase.equals("filesize")){
        if(words(i+1).equals(">") || words(i+1).equals(">=") || words(i+1).equals("<") || words(i+1).equals("<=")) {
          if (words(i + 1).equals(">")) {

            fileSizeLowBound = getBytes(words(i+2))
          }
          if (words(i + 1).equals(">=")) {
            fileSizeLowBoundCanEqual = getBytes(words(i+2))
          }
          if (words(i + 1).equals("<")) {
            fileSizeUpBound = getBytes(words(i+2))
          }
          if (words(i + 1).equals("<=")) {
            fileSizeUpBoundCanEqual = getBytes(words(i+2))
          }
          if (words.length>=(i+3+1)){
            //说明有and
            if(words(i+3).toLowerCase.equals("and")){
              if (words(i + 4).equals(">")) {
                fileSizeLowBound = getBytes(words(i+5))
              }
              if (words(i + 4).equals(">=")) {
                fileSizeLowBoundCanEqual = getBytes(words(i+5))
              }
              if (words(i + 4).equals("<")) {
                fileSizeUpBound = getBytes(words(i+5))
              }
              if (words(i + 4).equals("<=")) {
                fileSizeUpBoundCanEqual = getBytes(words(i+5))
              }
            }
          }
        }
        if(words(i+1).toLowerCase.equals("between")){
          fileSizeLowBoundCanEqual = getBytes(words(i+2))
          fileSizeUpBoundCanEqual = getBytes(words(i+3))
        }

      }
      if(words(i).toLowerCase.equals("totalsize")){
        if(words(i+1).equals(">") || words(i+1).equals(">=") || words(i+1).equals("<") || words(i+1).equals("<=")) {
          if (words(i + 1).equals(">")) {

            totalSizeLowBound = getBytes(words(i+2))
          }
          if (words(i + 1).equals(">=")) {
            totalSizeLowBoundCanEqual = getBytes(words(i+2))
          }
          if (words(i + 1).equals("<")) {
            totalSizeUpBound = getBytes(words(i+2))
          }
          if (words(i + 1).equals("<=")) {
            totalSizeUpBoundCanEqual = getBytes(words(i+2))
          }
          if (words.length>=(i+3+1)){
            //说明有and
            if(words(i+3).toLowerCase.equals("and")){
              if (words(i + 4).equals(">")) {
                totalSizeLowBound = getBytes(words(i+5))
              }
              if (words(i + 4).equals(">=")) {
                totalSizeLowBoundCanEqual = getBytes(words(i+5))
              }
              if (words(i + 4).equals("<")) {
                totalSizeUpBound = getBytes(words(i+5))
              }
              if (words(i + 4).equals("<=")) {
                totalSizeUpBoundCanEqual = getBytes(words(i+5))
              }
            }
          }
        }
        if(words(i+1).toLowerCase.equals("between")){
          fileSizeLowBoundCanEqual = getBytes(words(i+2))
          fileSizeUpBoundCanEqual = getBytes(words(i+3))
        }

      }
      if(words(i).toLowerCase.equals("time")){
        if(words(i+1).equals(">") || words(i+1).equals(">=") || words(i+1).equals("<") || words(i+1).equals("<=")) {
          if (words(i + 1).equals(">")) {
             timeLowBound = getTimeLong(words(i+2))
          }
          if (words(i + 1).equals(">=")) {
            timeLowBoundCanEqual = getTimeLong(words(i+2))
          }
          if (words(i + 1).equals("<")) {
            timeUpBound = getTimeLong(words(i+2))
          }
          if (words(i + 1).equals("<=")) {
            timeUpBoundCanEqual = getTimeLong(words(i+2))
          }
          if (words.length>=(i+3+1)){
            //说明有and
            if(words(i+3).toLowerCase.equals("and")){
              if (words(i + 4).equals(">")) {
                timeLowBound = getTimeLong(words(i+5))
              }
              if (words(i + 4).equals(">=")) {
                timeLowBoundCanEqual = getTimeLong(words(i+5))
              }
              if (words(i + 4).equals("<")) {
                timeUpBound = getTimeLong(words(i+5))
              }
              if (words(i + 4).equals("<=")) {
                timeUpBoundCanEqual = getTimeLong(words(i+5))
              }
            }
          }
        }
        if(words(i+1).toLowerCase.equals("between")){
          fileSizeLowBoundCanEqual = getBytes(words(i+2))
          fileSizeUpBoundCanEqual = getBytes(words(i+3))
        }

      }
    }
    parsePaths(inputPath)
    if(outPaths.size>=1){
      ss.read.format(inputType).load(outPaths:_*)
    }else{
      null
    }
  }

  def getDataFrameSimpleUnion(ss:SparkSession,args0:String): DataFrame ={
    println("FileSQL For Spark:::Waiting for execution SQL-->"+args0)
    //分析有几个union
    val sqls: Array[String] = args0.split("union")
    //传入并获得frame数组
    val frames: Array[DataFrame] = sqls.map(x=>getFilesInner(ss,x)).filter(x=>x!=null)
    println("FileSQL For Spark:::Your Selected Files-->"+outPathsShow.mkString("||"))
    println("FileSQL For Spark:::Your Input FilesTotalLength-->"+getFormatFileSize(fileSize))
    //去掉所有结构信息
   // implicit val matchError = org.apache.spark.sql.Encoders.tuple( Encoders.STRING, Encoders.STRING, Encoders.STRING, Encoders.STRING, Encoders.STRING)
    //注意对df里面的row进行操作的话先转成rdd,不然就要引入Encoders这种麻烦的东西
    val array: Array[RDD[Row]] = frames.map(x => x.rdd.map(y => y.toSeq).map(z => {
      var outString = ""
      for (p <- z) {
        if(p==null){
          outString = outString+null+"\001"
        }else{
          outString = outString + p.toString + "\001"
        }
      }
      outString
    }).map(k => Array(k)).map(p => Row.fromSeq(p)))
    val field = Array(DataTypes.createStructField("value",DataTypes.StringType,true))
    val frames1: Array[DataFrame] = array.map(x=>ss.createDataFrame(x,DataTypes.createStructType(field)))
    var standardFrames:DataFrame = null.asInstanceOf[DataFrame]
    if(frames1.size>=2){
      standardFrames = frames1.reduce((r1,r2)=>r1.union(r2))
    }else if(frames.size==1){
      standardFrames = frames1(0)
    }else{
      println("FileSQL For Spark:::No Files Has Been Selected!!! Check Your SQL")
    }
    standardFrames

  }

  def getDataFrameUnionByName(ss:SparkSession,args0:String,preStucts:ListBuffer[PreStruct]):DataFrame={
    println("FileSQL For Spark:::Waiting For Execution SQL-->"+args0)
    //分析有几个union,并获取单个sql的df数组
    val sqls: Array[String] = args0.split("union")
    val frames: Array[DataFrame] = sqls.map(x=>getFilesInner(ss,x))
    println("FileSQL For Spark:::Your Selected Files-->"+outPathsShow.mkString("||"))
    println("FileSQL For Spark:::Your Input FilesTotalLength-->"+getFormatFileSize(fileSize))

    //获取最全的表结构信息,放到mapOfNameAndType,因为有的数据源可能缺字段的
    val grandArray: Array[String] = preStucts.map(x=>x.fieldNameAndType).reduce((a1, a2)=>a1+","+a2).split(",").distinct
    val mapOfNameAndType:mutable.Map[String,String] = mutable.HashMap[String,String]()
    grandArray.map(x=>{
      mapOfNameAndType.put(x.split(":")(0),x.split(":")(1))
    })

    //将最全表格信息导入grandStructType
    val grandStructType: StructType = DataTypes.createStructType(mapOfNameAndType.map(x=>DataTypes.createStructField(x._1,parseDataType(x._2),true)).toArray)

    val framesWithTable:ArrayBuffer[DataFrame] = new ArrayBuffer[DataFrame]()

    for(i <- 0 until frames.length){
      if(preStucts(i).isInstanceOf[PreStructWithoutDelimits] && frames(i)!=null){

        //由于读取一些类型数据，如json数据不会按照json字段的顺序从前往后进行，故需要指定schema的顺序

        //先看一下schema里已经有什么字段了，为补全做准备
        var schema = frames(i).schema

        var sequence = schema.toList.map(x=>x.name).to[ListBuffer]
        val missingFieldMap: mutable.Map[String, String] = mapOfNameAndType.filter(x=>{schema.toList.map(y=>y.name).contains(x._1)==false})
    //    val missionStructFields=missingFieldMap.map(x=>DataTypes.createStructField(x._1,parseDataType(x._2),true))

        //根据缺失字段的数量补全null值
        val completedRDD: RDD[Row] = frames(i).rdd.map(x => x.toSeq).map(z => {
          val buffer: ListBuffer[Any] = z.to[ListBuffer]
          for (i <- 0 until missingFieldMap.size) {
            buffer += null
          }
          buffer
        }).map(k => Row.fromSeq(k))
        //根据缺失的字段补全sequence
        for(x<-missingFieldMap){
          sequence+=x._1
        }

        //json数据可能有一些字段是不存在的，不存在值的字段会读成null，需要解决这一个问题
        //rdd中出来的顺序是schema的顺序
       val outvalues:RDD[Row]=completedRDD.map(x => x.toSeq).map(z=>{
          val outList = ListBuffer[Any]()
          for(j<-0 until sequence.length){
            if(z(j)==null){
              outList.append(null)
            }else{
              outList.append(transDataType(mapOfNameAndType(sequence(j)),z(j).toString))
            }
          }
          outList
        }).map(z=>Row.fromSeq(z))

        //这里的grandStructType需要进行重新排序,这是因为json、orc、parquet等的schema是有序的,需要匹配上
        val list: List[StructField] = grandStructType.toList
        val tmpList = ListBuffer[StructField]()
        for(k<-sequence){
          for(j<-list){
            if(j.name.toString.equals(k)){
              tmpList.append(j)
            }
          }
        }
        val newStructType: StructType = DataTypes.createStructType(tmpList.toArray)
        framesWithTable.append(ss.createDataFrame(outvalues,newStructType))
      }else if(frames(i)!=null) {
        var tmpFrame = frames(i)
        if (tmpFrame != null) {
          //说明传了分隔符
          var delimits = preStucts(i).asInstanceOf[PreStructWithDelimits].delimits

          var tmpRDD = null.asInstanceOf[RDD[Array[String]]]

          //循环分隔符，将row类型拆分
          for (delimit <- delimits) {
            //  Array[String] x y z  =>  Row
            //  Array[String] x z k  =>  Row
            //  Array[String] a b c  =>  Row
            tmpRDD = tmpFrame.rdd.map(x => x.getString(0).split(delimit))
          }


          //被分割符分割后，除了新增的null值，所有的字段全部变成了String,需要将其格式化为相应的类型
          val stringTypes: Array[String] = preStucts(i).fieldNameAndType.split(",").map(x => x.split(":")(1))
          var index = 0
          val tmpRDD2: RDD[Array[Any]] = tmpRDD.map(x => x.map(y => {
            val outvalue: Any = transDataType(stringTypes(index), y)
            index = index + 1
            if (index == stringTypes.length) {
              index = 0
            }
            outvalue
          }))

          //同样的,也可能出现缺字段，无数据的情况，需要在tmpRDD中补全数据为null
          //获取缺失字段
          val reserveFields: Array[String] = preStucts(i).fieldNameAndType.split(",")
          val missingFields: Array[String] = grandArray.filter(x => reserveFields.contains(x) == false)
          //补全缺失字段
          var tmpRDD3: RDD[ArrayBuffer[Any]] = tmpRDD2.map(x => {
            var y = x.to[ArrayBuffer]
            for (m <- missingFields) {
              y += null
            }
            y
          })
          val completedRDD: RDD[Row] = tmpRDD3.map(x => Row.fromSeq(x))

          framesWithTable.append(ss.createDataFrame(completedRDD, grandStructType))
        }
      }

    }

    var outDF:DataFrame=null
    //按字段合并各个frame
    /**
      * spark版本2.3.0以下
      */
//     val list: List[String] = mapOfNameAndType.toList.map(x=>x._1)
//    val standardFrame:ArrayBuffer[DataFrame] = new ArrayBuffer[DataFrame]()
//    if(framesWithTable.size==1){
//      outDF= framesWithTable(0)
//    }else if(framesWithTable.size>1){
//      var index = 0
//      framesWithTable.foreach(x=>{
//        index=index+1
//        x.createTempView("table"+index)
//      })
//      for(x<- 1 until index+1){
//        standardFrame.append(ss.sql("select "+list.mkString(",")+" from table"+x))
//      }
//      outDF = standardFrame.reduce((r1,r2)=>r1.union(r2))
//
//   //   outDF = framesWithTable.reduce((r1, r2)=>r1.unionByName(r2))
//    }else if(framesWithTable==0){
//    }

    /**
      * spark版本2.3.0及以上
      */
    if(framesWithTable.size==1){
      outDF= framesWithTable(0)
    }else if(framesWithTable.size>1){
      outDF = framesWithTable.reduce((r1, r2)=>r1.unionByName(r2))
    }else if(framesWithTable==0){
    }

    //如果没有数据，传出null
    if(outDF==null){
      println("FileSQL For Spark:::No Files Has Been Selected!!! Check Your SQL")
    }
    outDF
  }

//  private def getDStream(sc:StreamingContext,)

  private def transDataType(inputDataType:String,inputData:String):Any= {
    inputDataType.toLowerCase match {
      case "binary" => inputData.getBytes()
      case "byte" => inputData.getBytes()(0)
      case "string" => inputData
      case "short" => inputData.toShort
      case "int" => inputData.toInt
      case "long" => inputData.toLong
      case "boolean" => inputData.toBoolean
      case "date" => Date.parse(inputData)
      case "timestamp" => inputData.toLong
      case "float" => inputData.toFloat
      case "double" => inputData.toDouble
      case "calendar" => inputData.toInt
      case _ => DataTypes.BinaryType
    }
  }

  private def parseDataType(inputDataType:String): DataType={
    inputDataType.toLowerCase match {
      case "binary" => DataTypes.BinaryType
      case "byte" => DataTypes.ByteType
      case "string" => DataTypes.StringType
      case "short" => DataTypes.ShortType
      case "int" => DataTypes.IntegerType
      case "long" => DataTypes.LongType
      case "boolean" => DataTypes.BooleanType
      case "date" => DataTypes.DateType
      case "timestamp" => DataTypes.TimestampType
      case "float" => DataTypes.FloatType
      case "double" => DataTypes.DoubleType
      case "calendar" => DataTypes.CalendarIntervalType
      case "null" => DataTypes.NullType
      case _ => DataTypes.BinaryType
    }
  }
  private def getTimeLong(inputTime:String):Long={
    //2019-2-3:1-3-5 最短
    //2019-10-12:12-33-23 最长
    if(inputTime.trim().length>=14 && inputTime.trim.length<=19){
      timeFormat = new SimpleDateFormat("yyyy-MM-dd:HH-mm-ss")
    }else if(inputTime.trim.length>=8 && inputTime.trim.length<=10){
      timeFormat = new SimpleDateFormat("yyyy-MM-dd")
    }else{
      println("FileSQL For Spark:::Your Time Format Is Wrong,Please Use yyyy-MM-dd:HH-mm-ss " +
        "Or yyyy-MM-dd")
      throw new IllegalArgumentException("Time Format Is Wrong")
    }

    val date: Date = timeFormat.parse(inputTime)
    date.getTime
  }
  private def getFormatFileSize(inputFileSize:Long):String={
    var outvalue = ""
    if(inputFileSize<1048576L){
      outvalue = inputFileSize +"Bytes"
    }else if(inputFileSize>=1048576L && inputFileSize<1073741824L){
      outvalue = inputFileSize.toDouble/(1024*1024) + "M"
    }else{
      outvalue = inputFileSize.toDouble/(1024*1024*1024) + "G"
    }
    outvalue
  }

  private def getBytes(inputSize:String):Long={
    val num = inputSize.substring(0,inputSize.length-1).toLong
    var finalBytes = 0L
    if(inputSize.toLowerCase.endsWith("b")){
      finalBytes = num
    }else if(inputSize.toLowerCase.endsWith("k")){
      finalBytes = num*1024
    }else if(inputSize.toLowerCase.endsWith("m")){
      finalBytes = num*1024*1024
    }else if(inputSize.toLowerCase.endsWith("g")){
      finalBytes = num*1024*1024*1024
    }
    finalBytes
  }

  private def parsePaths(inputPath:String):Unit={
    var tmpStatuss = new ListBuffer[FileStatus]()
    //先来判断一下传入的是文件还是目录
    if(fs.isDirectory(new Path(inputPath))==false){
      //传入的是文件,加入进去
      tmpStatuss.append(fs.getFileStatus(new Path(inputPath)))
    }else {
      //传入的是目录

      //递归的方式找文件过于粗放了,这里只向下一层，采取将文件夹作为文件的方式,文件夹的的size是该文件夹下递归所有文件的大小
      val recursiveFiles = fs.listFiles(new Path(inputPath), true)
      val pureFiles=new ListBuffer[FileStatus]  //分离掉目录的纯文件
      val pureFilesLength:mutable.Map[String,Long] = new mutable.HashMap[String,Long] //纯文件的长度列表
      //处理recursiveFiles,将目录视为文件，获得一层下所有的文件地址
      var tmpPath=""
      if(inputPath.endsWith("/")){
        tmpPath = inputPath.substring(0,inputPath.length-1)
      }else{
        tmpPath = inputPath
      }
      val dirMap:mutable.Map[String,Long] = new mutable.HashMap[String,Long]()
      //分类文件为纯文件还是目录
      while (recursiveFiles.hasNext) {
        val status: LocatedFileStatus = recursiveFiles.next()
        if (status.getPath.toString.matches(".*" + tmpPath + "/[^/]*/.*")){
            //获得目录名称,放入一个hashSet去重
          dirMap.put(status.getPath.toString.split(".*" + tmpPath+"/")(1).split("/")(0),0L)
        }else{
          //说明这个是纯文件
          pureFiles.append(status)
          pureFilesLength.put(status.getPath.toString,status.getLen)
        }
      }
      //统计目录下一层到底有多少文件数(纯文件和文件)
      val totalCount: Int = dirMap.size+pureFiles.size
      //对每一个dirMap中的目录,进行递归,然后获取这个目录下所有文件,将value更新为这些文件的长度总和
        //在这里也对时间进行过滤，不满足条件的放入exclude列表
       val exclude: ListBuffer[String] = ListBuffer[String]()
      dirMap.foreach(x=>{
        val values: RemoteIterator[LocatedFileStatus] = fs.listFiles(new Path(tmpPath+"/"+x._1),true)
        var size = 0L
        while(values.hasNext){
          val status: LocatedFileStatus = values.next()
          val time: Long = status.getModificationTime
          if(time < timeUpBound && time <= timeUpBoundCanEqual && time > timeLowBound && time >= timeLowBoundCanEqual){
            size = size+status.getLen
          }else{
            //不满足条件,将path放入exclude列表
           exclude.append(status.getPath.toString)
          }
        }
        dirMap.put(x._1,size)
      })


      //tmpOutPaths1,tmpOutPaths2先将满足条件的文件(纯文件和目录)放入这个两个List,然后比较其size总和和totalCount的大小
      //如果大小相同,直接只将inputPath加入到输入目录
      val tmpOutPaths1:ListBuffer[String] = ListBuffer[String]()
      val tmpOutPaths2:ListBuffer[String] = ListBuffer[String]()
      //先对pureFiles进行过滤，满足条件的加入tmpOutPaths
      if(uLikeRegex!=""){
        pureFiles.filter(x=>x.getModificationTime < timeUpBound && x.getModificationTime <= timeUpBoundCanEqual
        && x.getModificationTime > timeLowBound && x.getModificationTime >= timeLowBoundCanEqual)
          .filter(x => x.getPath.getName.matches(fileRegex))
            .filter(x=>x.getPath.getName.matches(uLikeRegex)==false)
          .filter(x => x.getLen < fileSizeUpBound && x.getLen <= fileSizeUpBoundCanEqual && x.getLen > fileSizeLowBound
            && x.getLen >= fileSizeLowBoundCanEqual)
          .foreach(x => {
            aggregateTotalSize = aggregateTotalSize +pureFilesLength(x.getPath.toString)
            if (num < limit && aggregateTotalSize<totalSizeUpBound && aggregateTotalSize<=totalSizeUpBoundCanEqual
            && aggregateTotalSize>totalSizeLowBound && aggregateTotalSize>=totalSizeLowBoundCanEqual) {
              tmpOutPaths1.append(x.getPath.toString)
              num = num + 1
            }else{
              aggregateTotalSize = aggregateTotalSize -pureFilesLength(x.getPath.toString)
            }
          })
      }else{
        pureFiles.filter(x=>x.getModificationTime < timeUpBound && x.getModificationTime <= timeUpBoundCanEqual
          && x.getModificationTime > timeLowBound && x.getModificationTime >= timeLowBoundCanEqual)
          .filter(x => x.getPath.getName.matches(fileRegex))
          .filter(x => x.getLen < fileSizeUpBound && x.getLen <= fileSizeUpBoundCanEqual && x.getLen > fileSizeLowBound
            && x.getLen >= fileSizeLowBoundCanEqual)
          .foreach(x => {
            aggregateTotalSize = aggregateTotalSize +pureFilesLength(x.getPath.toString)
            if (num < limit && aggregateTotalSize<totalSizeUpBound && aggregateTotalSize<=totalSizeUpBoundCanEqual
              && aggregateTotalSize>totalSizeLowBound && aggregateTotalSize>=totalSizeLowBoundCanEqual) {
              tmpOutPaths1.append(x.getPath.toString)
              num = num + 1
            }else{
              aggregateTotalSize = aggregateTotalSize -pureFilesLength(x.getPath.toString)
            }
          })
      }


      //再对dirMap进行过滤,满足条件的加入tmpOutPaths。同时加入目录列表
      if(uLikeRegex!=""){
        dirMap.filter(x=>x._1.matches(fileRegex)).filter(x=>x._1.matches(uLikeRegex)==false)
          .filter(x=>x._2 < fileSizeUpBound && x._2 <= fileSizeUpBoundCanEqual && x._2 > fileSizeLowBound
          && x._2 >= fileSizeLowBoundCanEqual)
          .foreach(x=>{
            aggregateTotalSize = aggregateTotalSize +dirMap(x._1)
            if (num < limit && aggregateTotalSize<totalSizeUpBound && aggregateTotalSize<=totalSizeUpBoundCanEqual
              && aggregateTotalSize>totalSizeLowBound && aggregateTotalSize>=totalSizeLowBoundCanEqual) {
              tmpOutPaths2.append(tmpPath+"/"+x._1)
              num = num + 1
            }else{
              aggregateTotalSize = aggregateTotalSize -dirMap(x._1)
            }
          })
      }else{
        dirMap.filter(x=>x._1.matches(fileRegex)).filter(x=>x._2 < fileSizeUpBound && x._2 <= fileSizeUpBoundCanEqual && x._2 > fileSizeLowBound
          && x._2 >= fileSizeLowBoundCanEqual)
          .foreach(x=>{
            aggregateTotalSize = aggregateTotalSize +dirMap(x._1)
            if (num < limit && aggregateTotalSize<totalSizeUpBound && aggregateTotalSize<=totalSizeUpBoundCanEqual
              && aggregateTotalSize>totalSizeLowBound && aggregateTotalSize>=totalSizeLowBoundCanEqual) {
              tmpOutPaths2.append(tmpPath+"/"+x._1)
              num = num + 1
            }else{
              aggregateTotalSize = aggregateTotalSize -dirMap(x._1)
            }
          })
      }

      if(tmpOutPaths1.size+tmpOutPaths2.size==totalCount){
        //说明全部为目标对象(文件加目录)
          //先将所有纯文件传入
        outPaths.append(inputPath)
        outPathsShow.append(inputPath)
          //统计纯文件的长度之和,加到全局变量fileSize
        fileSize = fileSize + pureFilesLength.values.sum
          //然后递归寻找目录下文件，也加入,注意这里要过滤掉exclude中的文件
        tmpOutPaths2.foreach(x=>{
          val value: RemoteIterator[LocatedFileStatus] = fs.listFiles(new Path(x),true)
          while(value.hasNext){
            val status: LocatedFileStatus = value.next()
            if(exclude.contains(status.getPath.toString)==false){
              outPaths.append(status.getPath.toString)
              outPathsShow.append(status.getPath.toString)
            }
          }
        })
          //统计目录的长度之和,加到全局变量fileSize,这里的dirMap已经过滤掉了exclude的size了,所以没关系
        fileSize = fileSize + dirMap.values.sum
      }else{
        tmpOutPaths1.foreach(x=>{
          outPaths.append(x)
          outPathsShow.append(x)
          fileSize = fileSize + pureFilesLength.filter(x=>tmpOutPaths1.contains(x._1)).values.sum
        })
        tmpOutPaths2.foreach(x=>{
          val value: RemoteIterator[LocatedFileStatus] = fs.listFiles(new Path(x),true)
          while(value.hasNext){
            val status: LocatedFileStatus = value.next()
            if(exclude.contains(status.getPath.toString)==false){
              outPaths.append(status.getPath.toString)
              outPathsShow.append(status.getPath.toString)
            }
          }
        })
        fileSize = fileSize + dirMap.filter(x=>tmpOutPaths2.contains(tmpPath+"/"+x._1)).values.sum
      }
    }

    }
}

//抽象类,定义表结构
abstract class PreStruct{
  val fieldNameAndType:String

}
//如果是Text这种格式，是需要传入delimits的
abstract class PreStructWithDelimits extends PreStruct{
  val delimits:List[String]

}
//因为有一些格式是不需要传入Delimits
abstract class PreStructWithoutDelimits extends PreStruct{
}