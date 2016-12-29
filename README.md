# HadoopEBCDIC
Parse Mainframe EBCDIC Files using Hadoop

command to execute in MR.

hadoop jar EBCDICInputFormat.jar com.ebcdic.mainframe.examples.EBCDICDriver <InputFile> <OutFile> <CopyBook x2cj>

use cb2xml to generate x2cj copybook file.
  cb2xc2j {copybookfile}
