package base

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	toolkits "my2sql/toolkits"
)

var (
	fileBinEventHandlingIndex uint64 = 0	// 当前已解析的 binlog 文件下标，从 0 开始
	fileTrxIndex              uint64 = 0	// 当前已解析的事务总数
)

type BinFileParser struct {
	Parser *replication.BinlogParser
}

// [root@10-186-61-119 binlog]# ll
// total 4579116
//	-rw-r----- 1 mysql mysql        209 Aug  3 14:17 mysql-bin.000010
//	-rw-r----- 1 mysql mysql 1073760482 Aug  3 14:32 mysql-bin.000011
//	-rw-r----- 1 mysql mysql 1074119415 Aug  3 14:36 mysql-bin.000012
//	-rw-r----- 1 mysql mysql 1073822542 Aug  3 15:53 mysql-bin.000013
//	-rw-r----- 1 mysql mysql 1074588226 Aug  3 16:15 mysql-bin.000014
//	-rw-r----- 1 mysql mysql  392707488 Aug  3 16:16 mysql-bin.000015
//	-rw-r----- 1 mysql mysql        246 Aug  3 16:15 mysql-bin.index


// MyParseAllBinlogFiles 逐个 binlog 文件进行解析，解析结果会通过 cfg 中的管道传出去。
func (this BinFileParser) MyParseAllBinlogFiles(cfg *ConfCmd) {
	defer cfg.CloseChan()
	log.Info("start to parse binlog from local files")

	// 提取配置
	binlog, binpos := GetFirstBinlogPosToParse(cfg)

	// 将 mysql3306-bin.000004 解析成 mysql3306-bin, 4
	binBaseName, binBaseIndx := GetBinlogBasenameAndIndex(binlog)
	log.Info(fmt.Sprintf("start to parse %s %d\n", binlog, binpos))

	for {
		// 如果设置了 stop pos ，会自动停止
		if cfg.IfSetStopFilePos {
			if cfg.StopFilePos.Compare(mysql.Position{Name: filepath.Base(binlog), Pos: 4}) < 1 {
				break
			}
		}
		log.Info(fmt.Sprintf("start to parse %s %d\n", binlog, binpos))
		// 解析 binlog 文件
		result, err := this.MyParseOneBinlogFile(cfg, binlog)
		if err != nil {
			log.Error(fmt.Sprintf("error to parse binlog %s %v", binlog, err))
			break
		}
		// 结束
		if result == C_reBreak {
			break
		// 文件尾
		} else if result == C_reFileEnd {
			if !cfg.IfSetStopParsPoint && !cfg.IfSetStopDateTime {
				//just parse one binlog
				break
			}
			// 继续解析下一个 binlog 文件
			binlog = filepath.Join(cfg.BinlogDir, GetNextBinlog(binBaseName, binBaseIndx))
			if !toolkits.IsFile(binlog) {
				log.Info(fmt.Sprintf("%s not exists nor a file\n", binlog))
				break
			}
			binBaseIndx++ 	// 索引下标 +1
			binpos = 4		// 重置 offset
		} else {
			log.Info(fmt.Sprintf("this should not happen: return value of MyParseOneBinlog is %d\n", result))
			break
		}
	}
	log.Info("finish parsing binlog from local files")
}

func (this BinFileParser) MyParseOneBinlogFile(cfg *ConfCmd, name string) (int, error) {
	// process: 0, continue: 1, break: 2
	// 打开文件
	f, err := os.Open(name)
	if f != nil {
		defer f.Close()
	}
	if err != nil {
		log.Error(fmt.Sprintf("fail to open %s %v\n", name, err))
		return C_reBreak, errors.Trace(err)
	}
	// 读取文件类型
	fileTypeBytes := int64(4)
	b := make([]byte, fileTypeBytes)
	if _, err = f.Read(b); err != nil {
		log.Error(fmt.Sprintf("fail to read %s %v", name, err))
		return C_reBreak, errors.Trace(err)
	} else if !bytes.Equal(b, replication.BinLogFileHeader) {
		// 判断是否为 binlog 文件类型
		log.Error(fmt.Sprintf("%s is not a valid binlog file, head 4 bytes must fe'bin' ", name))
		return C_reBreak, errors.Trace(err)
	}
	// must not seek to other position, otherwise the program may panic because formatevent, table map event is skipped
	// 跳过 4B
	if _, err = f.Seek(fileTypeBytes, os.SEEK_SET); err != nil {
		log.Error(fmt.Sprintf("error seek %s to %d", name, fileTypeBytes))
		return C_reBreak, errors.Trace(err)
	}
	// 执行解析
	var binlog string = filepath.Base(name)
	return this.MyParseReader(cfg, f, &binlog)
}


func (this BinFileParser) MyParseReader(cfg *ConfCmd, r io.Reader, binlog *string) (int, error) {
	// process: 0, continue: 1, break: 2, EOF: 3
	var (
		err         error
		n           int64
		db          string = ""
		tb          string = ""
		sql         string = ""
		sqlType     string = ""
		rowCnt      uint32 = 0
		trxStatus   int    = 0
		sqlLower    string = ""
		tbMapPos    uint32 = 0	//
	)

	for {
		// 读取 19B 事件头
		headBuf := make([]byte, replication.EventHeaderSize)
		if _, err = io.ReadFull(r, headBuf); err == io.EOF {
			return C_reFileEnd, nil
		} else if err != nil {
			log.Error(fmt.Sprintf("fail to read binlog event header of %s %v", *binlog, err))
			return C_reBreak, errors.Trace(err)
		}
		// 解析事件头
		var h *replication.EventHeader
		h, err = this.Parser.ParseHeader(headBuf)
		if err != nil {
			log.Error(fmt.Sprintf("fail to parse binlog event header of %s %v" , *binlog, err))
			return C_reBreak, errors.Trace(err)
		}
		//fmt.Printf("parsing %s %d %s\n", *binlog, h.LogPos, GetDatetimeStr(int64(h.Timestamp), int64(0), DATETIME_FORMAT))
		// 校验
		if h.EventSize <= uint32(replication.EventHeaderSize) {
			err = errors.Errorf("invalid event header, event size is %d, too small", h.EventSize)
			log.Error("%v", err)
			return C_reBreak, err
		}
		// 读取 size 事件体
		var buf bytes.Buffer
		if n, err = io.CopyN(&buf, r, int64(h.EventSize)-int64(replication.EventHeaderSize)); err != nil {
			err = errors.Errorf("get event body err %v, need %d - %d, but got %d", err, h.EventSize, replication.EventHeaderSize, n)
			log.Error("%v", err)
			return C_reBreak, err
		}

		//h.Dump(os.Stdout)
		// 拼接 head 和 body
		data := buf.Bytes()
		var rawData []byte
		rawData = append(rawData, headBuf...)
		rawData = append(rawData, data...)
		// 校验
		eventLen := int(h.EventSize) - replication.EventHeaderSize
		if len(data) != eventLen {
			err = errors.Errorf("invalid data size %d in event %s, less event length %d", len(data), h.EventType, eventLen)
			log.Errorf("%v", err)
			return C_reBreak, err
		}

		// 解析事件体
		var e replication.Event
		e, err = this.Parser.ParseEvent(h, data, rawData)
		if err != nil {
			log.Error(fmt.Sprintf("fail to parse binlog event body of %s %v",*binlog, err))
			return C_reBreak, errors.Trace(err)
		}

		// 基于 ROW 格式的 MySQL Binlog 在记录 DML 语句的数据时，总会先写入一个 table_map_event ，
		// 这种类型的 event 用于记录表结构相关元数据信息，比如数据库名称，表名称，表的字段类型，表的字段元数据等等。
		//
		// TABLE_MAP_EVENT 只有在 binlog 文件是以 ROW 格式记录的时候，才会使用。
		// binlog 中记录的每个更改的记录之前都会有一个对应要操作的表的 TABLE_MAP_EVENT 。
		// TABLE_MAP_EVENT 中记录了表的定义（包括数据库名称，表名称，表的字段类型定义），
		// 并且会将这个表的定义对应于一个数字，称为 table_id 。
		//
		// 设计 TABLE_MAP_EVENT 类型 event 的目的是为了当主库和从库之间有不同的表定义的时候，复制仍能进行。
		// 如果一个事务中操作了多个表，多行记录，在 binlog 中会将对多行记录的操作 event 进行分组，
		// 每组行记录操作 event 前面会出现对应表的 TABLE_MAP_EVENT 。
		//
		if h.EventType == replication.TABLE_MAP_EVENT {
			// ???
			tbMapPos = h.LogPos - h.EventSize // avoid mysqlbing mask the row event as unknown table row event
		}

		//e.Dump(os.Stdout)
		// can not advance this check, because we need to parse table map event or table may not found.
		// Also we must seek ahead the read file position
		//
		// 不能提前进行这个检查，因为我们需要解析表映射事件，否则可能找不到表。
		// 另外，我们必须提前寻找读取文件的位置。

		// 检查当前 event 是否应该被处理
		chRe := CheckBinHeaderCondition(cfg, h, *binlog)
		if chRe == C_reBreak {
			// 结束
			return C_reBreak, nil
		} else if chRe == C_reContinue {
			// 忽略，继续
			continue
		} else if chRe == C_reFileEnd {
			// 文件尾
			return C_reFileEnd, nil
		}

		//binEvent := &replication.BinlogEvent{RawData: rawData, Header: h, Event: e}

		// 创建 event 对象
		binEvent := &replication.BinlogEvent{
			Header: h,
			Event: e,
			// RawData: rawData, // we donnot need raw data
		}

		oneMyEvent := &MyBinEvent{
			MyPos: mysql.Position{
				Name: *binlog,
				Pos: h.LogPos,
			},
			StartPos: tbMapPos,
		}

		//StartPos: h.LogPos - h.EventSize}
		// 解析当前 event ，得到 RowsEvent 后保存到 oneMyEvent.BinEvent 上。
		chRe = oneMyEvent.CheckBinEvent(cfg, binEvent, binlog)
		if chRe == C_reBreak {
			// 停止遍历(时间区间非法)
			return C_reBreak, nil
		} else if chRe == C_reContinue {
			// 继续遍历(过滤)
			continue
		} else if chRe == C_reFileEnd {
			// 停止遍历(文件尾)
			return C_reFileEnd, nil
		}

		// 库, 表, 类型, 语句, 行数目
		db, tb, sqlType, sql, rowCnt = GetDbTbAndQueryAndRowCntFromBinevent(binEvent)

		// 查询
		if sqlType == "query" {
			sqlLower = strings.ToLower(sql)
			// 事务
			if sqlLower == "begin" {
				trxStatus = C_trxBegin
				fileTrxIndex++	// 事务号
			} else if sqlLower == "commit" {
				trxStatus = C_trxCommit
			} else if sqlLower == "rollback" {
				trxStatus = C_trxRollback
			} else if oneMyEvent.QuerySql != nil {
				trxStatus = C_trxProcess
				rowCnt = 1
			}
		} else {
			trxStatus = C_trxProcess
		}

		// 任务类型
		if cfg.WorkType != "stats" {

			// 是否需要发送
			ifSendEvent := false

			// 当前事件是 INSERT/UPDATE/DELETE 的 rows 事件
			if oneMyEvent.IfRowsEvent {
				// 构造库表名 db.tb
				tbKey := GetAbsTableName(string(oneMyEvent.BinEvent.Table.Schema), string(oneMyEvent.BinEvent.Table.Table))
				// 查询 db.tb 的表信息(字段、索引)
				_, err = G_TablesColumnsInfo.GetTableInfoJson(string(oneMyEvent.BinEvent.Table.Schema), string(oneMyEvent.BinEvent.Table.Table))
				if err != nil {
					log.Fatalf(fmt.Sprintf("no table struct found for %s, it maybe dropped, skip it. RowsEvent position:%s",
							tbKey, oneMyEvent.MyPos.String()))
				}
				// 需要将当前 event 发送出去
				ifSendEvent = true
			}

			// 发送到管道 cfg.EventChan 上
			if ifSendEvent {
				fileBinEventHandlingIndex++
				oneMyEvent.EventIdx = fileBinEventHandlingIndex
				oneMyEvent.SqlType = sqlType
				oneMyEvent.Timestamp = h.Timestamp
				oneMyEvent.TrxIndex = fileTrxIndex
				oneMyEvent.TrxStatus = trxStatus
				cfg.EventChan <- *oneMyEvent
			}
		}

		//output analysis result whatever the WorkType is
		//
		//
		if sqlType != "" {
			// 查询类型
			if sqlType == "query" {
				// 发送到管道 cfg.StatChan 上
				cfg.StatChan <- BinEventStats{
					Timestamp: h.Timestamp,
					Binlog: *binlog,
					StartPos: h.LogPos - h.EventSize, 	// ???
					StopPos: h.LogPos,
					Database: db,
					Table: tb,
					QuerySql: sql,
					RowCnt: rowCnt,
					QueryType: sqlType,
				}
			} else {
				cfg.StatChan <- BinEventStats{
					Timestamp: h.Timestamp,
					Binlog: *binlog,
					StartPos: tbMapPos,
					StopPos: h.LogPos,
					Database: db,
					Table: tb,
					QuerySql: sql,
					RowCnt: rowCnt,
					QueryType: sqlType,
				}
			}
		}
	}

	return C_reFileEnd, nil
}

