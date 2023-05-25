package base

import (
	"path/filepath"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/siddontang/go-log/log"
	"my2sql/dsql"
	toolkits "my2sql/toolkits"
)

type BinEventHandlingIndx struct {
	EventIdx uint64
	lock     sync.RWMutex
	Finished bool
}

var (
	G_HandlingBinEventIndex *BinEventHandlingIndx
)

type MyBinEvent struct {
	MyPos       mysql.Position         // this is the end position
	EventIdx    uint64                 //
	BinEvent    *replication.RowsEvent //
	StartPos    uint32                 // this is the start position
	IfRowsEvent bool                   // 当前事件是否是 INSERT/UPDATE/DELETE 的 rows 事件
	SqlType     string                 // insert, update, delete
	Timestamp   uint32                 //
	TrxIndex    uint64                 //
	TrxStatus   int                    // 0:begin, 1: commit, 2: rollback, -1: in_progress
	QuerySql    *dsql.SqlInfo          // for ddl and binlog which is not row format
	OrgSql      string                 // for ddl and binlog which is not row format
}

func (e *MyBinEvent) CheckBinEvent(cfg *ConfCmd, ev *replication.BinlogEvent, currentBinlog *string) int {
	// 当前 pos
	myPos := mysql.Position{
		Name: *currentBinlog,
		Pos:  ev.Header.LogPos,
	}

	// Rotate_event
	//
	// 当 MySQL 切换至新的 binlog 文件的时候，MySQL 会在旧的 binlog 文件中写入一个 ROTATE_EVENT ，
	// 其内容包含新的 binlog 文件的文件名以及第一个偏移地址。
	//
	// 当在数据库中执行 FLUSH LOGS 语句或者 binlog 文件的大小超过 max_binlog_size 参数设定的值就会切换新的 binlog 文件。
	//
	//
	// 当 binlog 文件超过指定大小（在哪指定？），ROTATE EVENT 会写在文件最后，指向下一个 binlog 文件。
	// 这个事件通知 slave ，应该去读取下一个 binlog 文件了。
	//
	// master 会写 ROTATE EVENT 到本地文件；在 slave 上运行 FLUSH LOGS 指令，或者收到 master 的 ROTATE EVENT 事件，
	// slave 会将 ROTATE EVENT 写到 relay log 里。
	//
	// 也存在 binlog 文件没有 ROTATE EVENT 的情况，比如 server crash 的时候。

	// 如果当前为 rotate 事件，则解析得到下一个 binlog 文件名，然后返回
	if ev.Header.EventType == replication.ROTATE_EVENT {
		rotateEvent := ev.Event.(*replication.RotateEvent)
		*currentBinlog = string(rotateEvent.NextLogName) // 下个文件
		e.IfRowsEvent = false
		return C_reContinue
	}

	// 过滤
	if cfg.IfSetStartFilePos {
		// 如果当前 myPos 小于 cfg.StartFilePos ，就 Continue 。
		cmpRe := myPos.Compare(cfg.StartFilePos)
		if cmpRe == -1 {
			return C_reContinue
		}
	}
	// 过滤
	if cfg.IfSetStopFilePos {
		// 如果当前 myPos 大于等于 cfg.StopFilePos ，就 Break 。
		cmpRe := myPos.Compare(cfg.StopFilePos)
		if cmpRe >= 0 {
			log.Infof("stop to get event. StopFilePos set. currentBinlog %s StopFilePos %s", myPos.String(), cfg.StopFilePos.String())
			return C_reBreak
		}
	}

	//fmt.Println(cfg.StartDatetime, cfg.StopDatetime, header.Timestamp)
	// 过滤
	if cfg.IfSetStartDateTime {
		// 如果当前 event 的时间早于 cfg.StartDatetime ，就 Continue 。
		if ev.Header.Timestamp < cfg.StartDatetime {
			return C_reContinue
		}
	}

	// 过滤
	if cfg.IfSetStopDateTime {
		// 如果当前 event 的时间晚于 cfg.StopDatetime ，就 Break 。
		if ev.Header.Timestamp >= cfg.StopDatetime {
			log.Infof("stop to get event. StopDateTime set. current event Timestamp %d Stop DateTime  Timestamp %d", ev.Header.Timestamp, cfg.StopDatetime)
			return C_reBreak
		}
	}

	// 不过滤
	if cfg.FilterSqlLen == 0 {
		goto BinEventCheck
	}

	// ???
	if ev.Header.EventType == replication.WRITE_ROWS_EVENTv1 || ev.Header.EventType == replication.WRITE_ROWS_EVENTv2 {
		if cfg.IsTargetDml("insert") {
			goto BinEventCheck
		} else {
			return C_reContinue
		}
	}

	if ev.Header.EventType == replication.UPDATE_ROWS_EVENTv1 || ev.Header.EventType == replication.UPDATE_ROWS_EVENTv2 {
		if cfg.IsTargetDml("update") {
			goto BinEventCheck
		} else {
			return C_reContinue
		}
	}

	if ev.Header.EventType == replication.DELETE_ROWS_EVENTv1 || ev.Header.EventType == replication.DELETE_ROWS_EVENTv2 {
		if cfg.IsTargetDml("delete") {
			goto BinEventCheck
		} else {
			return C_reContinue
		}
	}



BinEventCheck:

	switch ev.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1,
		replication.UPDATE_ROWS_EVENTv1,
		replication.DELETE_ROWS_EVENTv1,
		replication.WRITE_ROWS_EVENTv2,
		replication.UPDATE_ROWS_EVENTv2,
		replication.DELETE_ROWS_EVENTv2:

		wrEvent := ev.Event.(*replication.RowsEvent)
		db := string(wrEvent.Table.Schema) // 库
		tb := string(wrEvent.Table.Table)  // 表

		/*if !cfg.IsTargetTable(db, tb) {
			return C_reContinue
		}*/

		// 检查是否是目标 db ，不是则 continue
		if len(cfg.Databases) > 0 {
			if !toolkits.ContainsString(cfg.Databases, db) {
				return C_reContinue
			}
		}

		// 检查是否是目标 table ，不是则 continue
		if len(cfg.Tables) > 0 {
			if !toolkits.ContainsString(cfg.Tables, tb) {
				return C_reContinue
			}
		}

		// 检查是否是忽略 dbs ，是则 continue
		if len(cfg.IgnoreDatabases) > 0 {
			if toolkits.ContainsString(cfg.IgnoreDatabases, db) {
				return C_reContinue
			}
		}

		// 检查是否是忽略 tables ，是则 continue
		if len(cfg.IgnoreTables) > 0 {
			if toolkits.ContainsString(cfg.IgnoreTables, tb) {
				return C_reContinue
			}
		}

		//
		e.BinEvent = wrEvent
		e.IfRowsEvent = true
	case replication.QUERY_EVENT:
		e.IfRowsEvent = false
	case replication.XID_EVENT:
		e.IfRowsEvent = false
	case replication.MARIADB_GTID_EVENT:
		e.IfRowsEvent = false
	default:
		e.IfRowsEvent = false
		return C_reContinue
	}

	return C_reProcess

}

func CheckBinHeaderCondition(cfg *ConfCmd, header *replication.EventHeader, currentBinlog string) int {
	// process: 0, continue: 1, break: 2

	// 构造当前 pos ，由文件名、偏移地址构成
	myPos := mysql.Position{Name: currentBinlog, Pos: header.LogPos}
	//fmt.Println(cfg.StartFilePos, cfg.IfSetStopFilePos, myPos)

	// 如果设置了解析的起始地址，且当前 pos 小于 start ，直接返回
	if cfg.IfSetStartFilePos {
		cmpRe := myPos.Compare(cfg.StartFilePos)
		if cmpRe == -1 {
			return C_reContinue
		}
	}

	// 如果设置了解析的结束地址，且当前 pos 大于 end ，直接返回
	if cfg.IfSetStopFilePos {
		cmpRe := myPos.Compare(cfg.StopFilePos)
		if cmpRe >= 0 {
			return C_reBreak
		}
	}

	//fmt.Println(cfg.StartDatetime, cfg.StopDatetime, header.Timestamp)
	//
	// 如果设置了解析的起始时间，且当前 timestamp 小于 start ，直接返回
	if cfg.IfSetStartDateTime {
		if header.Timestamp < cfg.StartDatetime {
			return C_reContinue
		}
	}

	// 如果设置了解析的结束时间，且当前 timestamp 大于 end ，直接返回
	if cfg.IfSetStopDateTime {
		if header.Timestamp >= cfg.StopDatetime {
			return C_reBreak
		}
	}

	// 过滤 sql 长度？
	if cfg.FilterSqlLen == 0 {
		return C_reProcess
	}

	// 'INSERT'
	if header.EventType == replication.WRITE_ROWS_EVENTv1 || header.EventType == replication.WRITE_ROWS_EVENTv2 {
		if cfg.IsTargetDml("insert") {
			return C_reProcess
		} else {
			return C_reContinue
		}
	}

	// 'UPDATE'
	if header.EventType == replication.UPDATE_ROWS_EVENTv1 || header.EventType == replication.UPDATE_ROWS_EVENTv2 {
		if cfg.IsTargetDml("update") {
			return C_reProcess
		} else {
			return C_reContinue
		}
	}

	// 'DELETE'
	if header.EventType == replication.DELETE_ROWS_EVENTv1 || header.EventType == replication.DELETE_ROWS_EVENTv2 {
		if cfg.IsTargetDml("delete") {
			return C_reProcess
		} else {
			return C_reContinue
		}
	}

	return C_reProcess
}

func GetFirstBinlogPosToParse(cfg *ConfCmd) (string, int64) {
	var binlog string
	var pos int64

	// 如果指定了起始 binlog 文件
	if cfg.StartFile != "" {
		binlog = filepath.Join(cfg.BinlogDir, cfg.StartFile)
	} else {
		binlog = cfg.GivenBinlogFile // 未指定，则读取指定 binlog 文件
	}

	// 如果指定了起始偏移
	if cfg.StartPos != 0 {
		pos = int64(cfg.StartPos)
	} else {
		pos = 4 // 未指定，默认 4
	}

	return binlog, pos
}
