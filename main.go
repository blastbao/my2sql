package main

import (
	"sync"

	"github.com/go-mysql-org/go-mysql/replication"
	my "my2sql/base"
)

func main() {
	my.GConfCmd.IfSetStopParsPoint = false
	my.GConfCmd.ParseCmdOptions()
	defer my.GConfCmd.CloseFH()
	if my.GConfCmd.WorkType != "stats" {
		my.G_HandlingBinEventIndex = &my.BinEventHandlingIndx{EventIdx: 1, Finished: false}
	}
	var wg, wgGenSql sync.WaitGroup
	wg.Add(1)
	go my.ProcessBinEventStats(my.GConfCmd, &wg)

	if my.GConfCmd.WorkType != "stats" {
		wg.Add(1)
		go my.PrintExtraInfoForForwardRollbackupSql(my.GConfCmd, &wg)
		for i := uint(1); i <= my.GConfCmd.Threads; i++ {
			wgGenSql.Add(1)
			go my.GenForwardRollbackSqlFromBinEvent(i, my.GConfCmd, &wgGenSql)
		}
	}



	// repl：伪装成从库从主库获取 binlog 文件
	if my.GConfCmd.Mode == "repl" {
		my.ParserAllBinEventsFromRepl(my.GConfCmd)
	// file：从本地文件系统获取 binlog 文件
	} else if my.GConfCmd.Mode == "file" {
		psr := replication.NewBinlogParser()
		psr.SetParseTime(false)	// do not parse mysql datetime/time column into go time structure, take it as string
		psr.SetUseDecimal(false)	// sqlbuilder not support decimal type
		my.BinFileParser{
			Parser: psr,
		}.MyParseAllBinlogFiles(my.GConfCmd)
	}
	wgGenSql.Wait()
	close(my.GConfCmd.SqlChan)
	wg.Wait() 
}



