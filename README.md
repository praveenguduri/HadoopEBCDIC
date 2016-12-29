# HadoopEBCDIC
Parse Mainframe EBCDIC Files with copybook in hadoop

The program uses cobol2j to parse the mainframe files.

http://cobol2j.sourceforge.net/
https://github.com/kinow/cobol2js

command to execute in MR.

    hadoop jar EBCDICInputFormat.jar com.ebcdic.mainframe.examples.EBCDICDriver "InputFile" "OutFile" {CopyBook x2cj}

use cb2xml to generate x2cj copybook file.

    cb2xc2j {copybookfile}

Code supports multi layout copybook


sample x2cj file 

     < FileFormat ConversionTable="Cp037" dataFileImplementation="RecTypeOffset~13~2" distinguishFieldSize="4" newLineSize="0">
        < RecordFormat cobolRecordName="RECORD-TYPE-1" distinguishFieldValue="AB">     
     ....
      ...

    dataFileImplementation is used to define the byte position to identify the record type.

      distinguishFieldValue - Record Type Value

      distinguishFieldSize-   Record Type size in bytes
