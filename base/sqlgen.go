package base

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/siddontang/go-log/log"
	SQL "my2sql/sqlbuilder"
	toolkits "my2sql/toolkits"
	"strings"
)

var G_Bytes_Column_Types []string = []string{"blob", "json", "geometry", C_unknownColType}

func GetPosStr(name string, spos uint32, epos uint32) string {
	return fmt.Sprintf("%s %d-%d", name, spos, epos)
}

func GetDroppedFieldName(idx int) string {
	return fmt.Sprintf("%s%d", C_unknownColPrefix, idx)
}

// GetAllFieldNamesWithDroppedFields
//
// 	rowLen: 当前 row 的列数目
//  colNames: 数据库表定义中的列数目
func GetAllFieldNamesWithDroppedFields(rowLen int, colNames []FieldInfo) []FieldInfo {
	// 不匹配，返回定义的列数目
	if rowLen <= len(colNames) {
		return colNames
	}

	// 至此，rowlen 大于等于 len(colNames) ...
	var arr []FieldInfo = make([]FieldInfo, rowLen)

	// 先拷贝 0..len(colNames)
	cnt := copy(arr, colNames)

	// 再填入空字段 len(colNames) .. rowlen
	for i := cnt; i < rowLen; i++ {
		arr[i] = FieldInfo{
			FieldName: GetDroppedFieldName(i - cnt),	// 字段名
			FieldType: C_unknownColType,				// 字段类型
		}
	}

	return arr
}

// GetSqlFieldsEXpressions
//
// 	colCnt: 列数目
//  colNames: 列信息
//  tbMap:
func GetSqlFieldsEXpressions(colCnt int, colNames []FieldInfo, tbMap *replication.TableMapEvent) ([]SQL.NonAliasColumn, []string) {

	colDefExps := make([]SQL.NonAliasColumn, colCnt)
	colTypeNames := make([]string, colCnt)


	for i := 0; i < colCnt; i++ {

		//
		typeName, colDef := GetMysqlDataTypeNameAndSqlColumn(
			colNames[i].FieldType,	// 字段类型
			colNames[i].FieldName,	// 字段名
			tbMap.ColumnType[i],	// 表变更事件：列类型
			tbMap.ColumnMeta[i],	// 表变更事件：列元数据
		)

		colDefExps[i] = colDef
		colTypeNames[i] = typeName

	}


	return colDefExps, colTypeNames
}

// GetMysqlDataTypeNameAndSqlColumn
// [!!!]
func GetMysqlDataTypeNameAndSqlColumn(
	tpDef string,		//
	colName string,		//
	tp byte,			//
	meta uint16,		//
) (
	string,
	SQL.NonAliasColumn,
) {

	// for unkown type, defaults to BytesColumn

	// get real string type
	if tp == mysql.MYSQL_TYPE_STRING {
		if meta >= 256 {
			b0 := uint8(meta >> 8)
			if b0&0x30 != 0x30 {
				tp = byte(b0 | 0x30)
			} else {
				tp = b0
			}
		}
	}

	//fmt.Println("column type:", colName, tp)
	switch tp {
	case mysql.MYSQL_TYPE_NULL:
		return C_unknownColType, SQL.BytesColumn(colName, SQL.NotNullable)
	case mysql.MYSQL_TYPE_LONG:
		return "int", SQL.IntColumn(colName, SQL.NotNullable)

	case mysql.MYSQL_TYPE_TINY:
		return "tinyint", SQL.IntColumn(colName, SQL.NotNullable)

	case mysql.MYSQL_TYPE_SHORT:
		return "smallint", SQL.IntColumn(colName, SQL.NotNullable)

	case mysql.MYSQL_TYPE_INT24:
		return "mediumint", SQL.IntColumn(colName, SQL.NotNullable)

	case mysql.MYSQL_TYPE_LONGLONG:
		return "bigint", SQL.IntColumn(colName, SQL.NotNullable)

	case mysql.MYSQL_TYPE_NEWDECIMAL:
		return "decimal", SQL.DoubleColumn(colName, SQL.NotNullable)

	case mysql.MYSQL_TYPE_FLOAT:
		return "float", SQL.DoubleColumn(colName, SQL.NotNullable)
	case mysql.MYSQL_TYPE_DOUBLE:
		return "double", SQL.DoubleColumn(colName, SQL.NotNullable)
	case mysql.MYSQL_TYPE_BIT:
		return "bit", SQL.IntColumn(colName, SQL.NotNullable)
	case mysql.MYSQL_TYPE_TIMESTAMP:
		//return "timestamp", SQL.DateTimeColumn(colName, SQL.NotNullable)
		return "timestamp", SQL.StrColumn(colName, SQL.UTF8, SQL.UTF8CaseInsensitive, SQL.NotNullable)
	case mysql.MYSQL_TYPE_TIMESTAMP2:
		//return "timestamp", SQL.DateTimeColumn(colName, SQL.NotNullable)
		return "timestamp", SQL.StrColumn(colName, SQL.UTF8, SQL.UTF8CaseInsensitive, SQL.NotNullable)
	case mysql.MYSQL_TYPE_DATETIME:
		//return "datetime", SQL.DateTimeColumn(colName, SQL.NotNullable)
		return "timestamp", SQL.StrColumn(colName, SQL.UTF8, SQL.UTF8CaseInsensitive, SQL.NotNullable)
	case mysql.MYSQL_TYPE_DATETIME2:
		//return "datetime", SQL.DateTimeColumn(colName, SQL.NotNullable)
		return "timestamp", SQL.StrColumn(colName, SQL.UTF8, SQL.UTF8CaseInsensitive, SQL.NotNullable)
	case mysql.MYSQL_TYPE_TIME:
		return "time", SQL.StrColumn(colName, SQL.UTF8, SQL.UTF8CaseInsensitive, SQL.NotNullable)
	case mysql.MYSQL_TYPE_TIME2:
		return "time", SQL.StrColumn(colName, SQL.UTF8, SQL.UTF8CaseInsensitive, SQL.NotNullable)
	case mysql.MYSQL_TYPE_DATE:
		return "date", SQL.StrColumn(colName, SQL.UTF8, SQL.UTF8CaseInsensitive, SQL.NotNullable)

	case mysql.MYSQL_TYPE_YEAR:
		return "year", SQL.IntColumn(colName, SQL.NotNullable)
	case mysql.MYSQL_TYPE_ENUM:
		return "enum", SQL.IntColumn(colName, SQL.NotNullable)
	case mysql.MYSQL_TYPE_SET:
		return "set", SQL.IntColumn(colName, SQL.NotNullable)
	case mysql.MYSQL_TYPE_BLOB:
		//text is stored as blob
		if strings.Contains(strings.ToLower(tpDef), "text") {
			return "blob", SQL.StrColumn(colName, SQL.UTF8, SQL.UTF8CaseInsensitive, SQL.NotNullable)
		}
		return "blob", SQL.BytesColumn(colName, SQL.NotNullable)
	case mysql.MYSQL_TYPE_VARCHAR,
		mysql.MYSQL_TYPE_VAR_STRING:

		return "varchar", SQL.StrColumn(colName, SQL.UTF8, SQL.UTF8CaseInsensitive, SQL.NotNullable)
	case mysql.MYSQL_TYPE_STRING:
		return "char", SQL.StrColumn(colName, SQL.UTF8, SQL.UTF8CaseInsensitive, SQL.NotNullable)
	case mysql.MYSQL_TYPE_JSON:
		//return "json", SQL.BytesColumn(colName, SQL.NotNullable)
		return "json", SQL.StrColumn(colName, SQL.UTF8, SQL.UTF8CaseInsensitive, SQL.NotNullable)

	case mysql.MYSQL_TYPE_GEOMETRY:
		return "geometry", SQL.BytesColumn(colName, SQL.NotNullable)
	default:
		return C_unknownColType, SQL.BytesColumn(colName, SQL.NotNullable)
	}
}

func GenInsertSqlsForOneRowsEvent(
	posStr string,
	rEv *replication.RowsEvent,
	colDefs []SQL.NonAliasColumn,
	rowsPerSql int,						// 一条 sql 负责几个 rows 的插入
	ifRollback bool,
	ifprefixDb bool,
	ifIgnorePrimary bool,
	primaryIdx []int,
) []string {

	var (
		insertSql  SQL.InsertStatement
		oneSql     string
		err        error
		i          int
		endIndex   int
		newColDefs []SQL.NonAliasColumn = colDefs[:]
		rowCnt     int                  = len(rEv.Rows)
		schema     string               = string(rEv.Table.Schema)
		table      string               = string(rEv.Table.Table)
		sqlArr     []string
		sqlType    string
	)

	if ifRollback {
		sqlType = "insert_for_delete_rollback"
		ifIgnorePrimary = false
	} else {
		sqlType = "insert"
	}

	// ???
	if len(primaryIdx) == 0 {
		ifIgnorePrimary = false
	}

	// 忽略主键
	if ifIgnorePrimary {
		// 移除主键对应的 ColDefs
		newColDefs = GetColDefIgnorePrimary(colDefs, primaryIdx)
	}

	// INSERT INTO table_name (column1,column2,column3,...)
	// VALUES (value1,value2,value3,...);

	// 每次处理 rowsPerSql 个 rows ，生成一条插入语句
	for i = 0; i < rowCnt; i += rowsPerSql {
		// 构造插入语句: `INSERT INTO table_name (column1,column2,column3,...) VALUES `
		insertSql = SQL.NewTable(table, newColDefs...).Insert(newColDefs...)

		// 边界处理，最后一批 rows
		endIndex = GetMinValue(rowCnt, i+rowsPerSql)

		// 把 rEv.Rows[i:endIndex] 填入到 insertSql 中，生成插入语句
		oneSql, err = GenInsertSqlForRows(
			rEv.Rows[i:endIndex], 	// (value1,value2,value3,...), (value1,value2,value3,...), ...
			insertSql,				//
			schema,					// database
			ifprefixDb,				//
			ifIgnorePrimary,		//
			primaryIdx,				//
		)
		if err != nil {
			log.Fatalf(fmt.Sprintf("Fail to generate %s sql for %s %s \n\terror: %v\n\trows data:%v",
				sqlType, GetAbsTableName(schema, table), posStr, err, rEv.Rows[i:endIndex]))
		} else {
			// 保存生成的语句
			sqlArr = append(sqlArr, oneSql)
		}
	}

	// 剩余 rows 处理一下 （应该不会出现?)
	if endIndex < rowCnt {
		insertSql = SQL.NewTable(table, newColDefs...).Insert(newColDefs...)
		oneSql, err = GenInsertSqlForRows(rEv.Rows[endIndex:rowCnt], insertSql, schema, ifprefixDb, ifIgnorePrimary, primaryIdx)
		if err != nil {
			log.Fatalf(fmt.Sprintf("Fail to generate %s sql for %s %s \n\terror: %s\n\trows data:%v",
				sqlType, GetAbsTableName(schema, table), posStr, err, rEv.Rows[endIndex:rowCnt]))
		} else {
			sqlArr = append(sqlArr, oneSql)
		}
	}

	//fmt.Println("one insert sqlArr", sqlArr)
	return sqlArr

}

func GetColDefIgnorePrimary(colDefs []SQL.NonAliasColumn, primaryIdx []int) []SQL.NonAliasColumn {
	m := []SQL.NonAliasColumn{}
	for i := range colDefs {
		// 忽略主键
		if toolkits.ContainsInt(primaryIdx, i) {
			continue
		}
		m = append(m, colDefs[i])
	}
	return m
}

//
func ConvertRowToExpressRow(row []interface{}, ifIgnorePrimary bool, primaryIdx []int) []SQL.Expression {
	valueInserted := []SQL.Expression{}
	for i, val := range row {
		// 忽略主键对应的列
		if ifIgnorePrimary {
			if toolkits.ContainsInt(primaryIdx, i) {
				continue
			}
		}
		//
		vExp := SQL.Literal(val)
		valueInserted = append(valueInserted, vExp)
	}
	return valueInserted
}

func GenInsertSqlForRows(
	rows [][]interface{},
	insertSql SQL.InsertStatement,
	schema string,
	ifprefixDb bool,
	ifIgnorePrimary bool,
	primaryIdx []int,
) (
	string,
	error,
) {

	// 遍历 rows ，填入 insertSql 中
	for _, row := range rows {
		valuesInserted := ConvertRowToExpressRow(row, ifIgnorePrimary, primaryIdx)
		insertSql.Add(valuesInserted...)
	}

	if !ifprefixDb {
		schema = ""
	}

	return insertSql.String(schema)

}

func GenDeleteSqlsForOneRowsEventRollbackInsert(
	posStr string,
	rEv *replication.RowsEvent,
	colDefs []SQL.NonAliasColumn,
	uniKey []int,
	ifFullImage bool,
	ifprefixDb bool,
) []string {

	return GenDeleteSqlsForOneRowsEvent(posStr, rEv, colDefs, uniKey, ifFullImage, true, ifprefixDb)

}



// GenDeleteSqlsForOneRowsEvent
//
// 该函数的作用是生成针对单行事件的删除 SQL 语句。
//
// 参数：
//	posStr：字符串类型，表示位置信息。
//	rEv：*replication.RowsEvent 类型，用于获取行事件相关信息。
//	colDefs：[]SQL.NonAliasColumn 类型，表示非别名列定义信息。
//	uniKey：[]int 类型，表示唯一键。
//	ifFullImage：布尔类型，表示是否使用全量镜像。
//	ifRollback：布尔类型，表示是否回滚。
//	ifprefixDb：布尔类型，表示是否添加数据库前缀。
//
// 返回值：
//  []string：字符串切片类型，表示生成的 SQL 语句数组。
func GenDeleteSqlsForOneRowsEvent(
	posStr string,
	rEv *replication.RowsEvent,
	colDefs []SQL.NonAliasColumn,
	uniKey []int,						// 唯一 key
	ifFullImage bool,
	ifRollback bool,
	ifprefixDb bool,
) []string {

	rowCnt := len(rEv.Rows)
	sqlArr := make([]string, rowCnt)
	//var sqlArr []string
	schema := string(rEv.Table.Schema)
	table := string(rEv.Table.Table)
	schemaInSql := schema
	if !ifprefixDb {
		schemaInSql = ""
	}

	// SQL 类型：delete
	var sqlType string
	if ifRollback {
		sqlType = "delete_for_insert_rollback"
	} else {
		sqlType = "delete"
	}

	// 遍历每个 row
	for i, row := range rEv.Rows {
		// 生成 WHERE 子句中的相等条件表达式集合，用 AND 组合起来
		whereCond := SQL.And(GenEqualConditions(row, colDefs, uniKey, ifFullImage)...)
		// 调用 String(schema) 方法将生成的 SQL 语句转换为字符串表示形式
		sql, err := SQL.NewTable(table, colDefs...).Delete().Where(whereCond).String(schemaInSql)
		if err != nil {
			log.Fatalf(fmt.Sprintf("Fail to generate %s sql for %s %s \n\terror: %s\n\trows data:%v", sqlType, GetAbsTableName(schema, table), posStr, err, row))
			//continue
		}
		// 将生成的 sql 语句保存到 sqlArr 中
		sqlArr[i] = sql
		//sqlArr = append(sqlArr, sql)
	}

	return sqlArr
}

func GenEqualConditions(row []interface{}, colDefs []SQL.NonAliasColumn, uniKey []int, ifFullImage bool) []SQL.BoolExpression {
	// 如果指定了 uniKey 且无需生成 full image ，就根据 uniKey 生成 where 条件，即可唯一定位到 row 。
	if !ifFullImage && len(uniKey) > 0 {
		expArrs := make([]SQL.BoolExpression, len(uniKey))
		for k, idx := range uniKey {
			// colDefs[idx] => unique key column name
			// row[idx]     => unique key column value
			expArrs[k] = SQL.EqL(colDefs[idx], row[idx])
		}
		return expArrs
	}

	// 否则，用 row 中每个 columns 一起来构造 where 条件。
	expArrs := make([]SQL.BoolExpression, len(row))
	for i, v := range row {
		expArrs[i] = SQL.EqL(colDefs[i], v)
	}

	return expArrs
}

func GenInsertSqlsForOneRowsEventRollbackDelete(posStr string, rEv *replication.RowsEvent, colDefs []SQL.NonAliasColumn, rowsPerSql int, ifprefixDb bool) []string {
	return GenInsertSqlsForOneRowsEvent(posStr, rEv, colDefs, rowsPerSql, true, ifprefixDb, false, []int{})
}

func GenUpdateSqlsForOneRowsEvent(
	posStr string,
	colsTypeNameFromMysql []string,
	colsTypeName []string,
	rEv *replication.RowsEvent,
	colDefs []SQL.NonAliasColumn,
	uniKey []int,
	ifFullImage bool,
	ifRollback bool,  // 如果为 true ，则意味着生成 update 的回滚语句
	ifprefixDb bool,
) []string {

	//colsTypeNameFromMysql: for text type, which is stored as blob
	var (
		rowCnt      int    = len(rEv.Rows)
		schema      string = string(rEv.Table.Schema)
		table       string = string(rEv.Table.Table)
		schemaInSql string = schema
		sqlArr      []string
		sql         string
		err         error
		sqlType     string
		wherePart   []SQL.BoolExpression
	)

	if !ifprefixDb {
		schemaInSql = ""
	}

	//
	// UPDATE table_name
	// SET column1 = value1, column2 = value2, ...
	// WHERE condition;
	//

	if ifRollback {
		sqlType = "update_for_update_rollback"
	} else {
		sqlType = "update"
	}

	// 对于 update 语句，会记录变更前后的 row 值，即 rows[0] 为 before ，rows[1] 为 after 。
	for i := 0; i < rowCnt; i += 2 {
		upSql := SQL.NewTable(table, colDefs...).Update() // ... UPDATE table_name ...
		if ifRollback {
			upSql = GenUpdateSetPart(colsTypeNameFromMysql, colsTypeName, upSql, colDefs, rEv.Rows[i], rEv.Rows[i+1], ifFullImage)
			wherePart = GenEqualConditions(rEv.Rows[i+1], colDefs, uniKey, ifFullImage)
		} else {
			upSql = GenUpdateSetPart(colsTypeNameFromMysql, colsTypeName, upSql, colDefs, rEv.Rows[i+1], rEv.Rows[i], ifFullImage)
			wherePart = GenEqualConditions(rEv.Rows[i], colDefs, uniKey, ifFullImage)
		}
		// 设置 where 条件
		upSql.Where(SQL.And(wherePart...))
		// 生成 sql 语句
		sql, err = upSql.String(schemaInSql)
		if err != nil {
			log.Fatalf(fmt.Sprintf("Fail to generate %s sql for %s %s \n\terror: %s\n\trows data:%v\n%v",
				sqlType, GetAbsTableName(schema, table), posStr, err, rEv.Rows[i], rEv.Rows[i+1]))
		} else {
			sqlArr = append(sqlArr, sql)
		}
	}
	//fmt.Println(sqlArr)
	return sqlArr

}


// GenUpdateSetPart 根据 rowAfter 和 rowBefore 中有差异的 columns 列值，生成 update 语句，用于将 row 更新为 after 。
func GenUpdateSetPart(
	colsTypeNameFromMysql []string,	// 列类型名集合
	colTypeNames []string,			// 列类型名集合
	updateSql SQL.UpdateStatement,	// SQL 语句
	colDefs []SQL.NonAliasColumn,	// 列名集合
	rowAfter []interface{},			//
	rowBefore []interface{},		//
	ifFullImage bool,				// 如果为 true ，就不考虑具体发生变更的 cols ，而是直接根据 rowAfter 生成完整的 sql 语句。
) SQL.UpdateStatement {

	ifColUpdated := false

	for colIdx, colVal := range rowAfter {

		// 当前 col 值在 before/after 中是否发生改变
		ifColUpdated = false

		//fmt.Printf("type: %s\nbefore: %v\nafter: %v\n", colTypeNames[i], rowBefore[i], v)

		// 如果为 true ，就不考虑具体发生变更的 cols ，而是直接根据 rowAfter 生成完整的 sql 语句。
		if !ifFullImage {
			// text is stored as blob in binlog
			// 如果列类型是 "blob", "json", "geometry", "unknown_type" 之一，且非 text 类型，则用特殊方式来比较
			if toolkits.ContainsString(G_Bytes_Column_Types, colTypeNames[colIdx]) &&
				!strings.Contains(strings.ToLower(colsTypeNameFromMysql[colIdx]), "text") {

				// 变更后的列值
				afterColVal, aOk := colVal.([]byte)
				// 变更前的列值
				beforeColVal, bOk := rowBefore[colIdx].([]byte)

				// 是否发生变化
				if aOk && bOk {
					if CompareEquelByteSlice(afterColVal, beforeColVal) {
						//fmt.Println("bytes compare equal")
						ifColUpdated = false
					} else {
						ifColUpdated = true
						//fmt.Println("bytes compare unequal")
					}
				} else {
					//fmt.Println("error to convert to []byte")
					//should update the column
					ifColUpdated = true
				}

			// 否则，直接用 "==" 来笔记
			} else {
				if colVal == rowBefore[colIdx] {
					//fmt.Println("compare equal")
					ifColUpdated = false
				} else {
					//fmt.Println("compare unequal")
					ifColUpdated = true
				}
			}
		} else {
			ifColUpdated = true
		}


		// UPDATE table_name
		// SET column1 = value1, column2 = value2, ...
		// WHERE condition;
		//
		// 如果列值发生变更，则需要更新指定列为 after col val 。
		if ifColUpdated {
			updateSql.Set(colDefs[colIdx], SQL.Literal(colVal))
		}
	}

	return updateSql

}
