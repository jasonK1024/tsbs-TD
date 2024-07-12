package tdengine_client

// TDengine 数据类型
const (
	TSDB_DATA_TYPE_NULL       = 0  // 1 bytes
	TSDB_DATA_TYPE_BOOL       = 1  // 1 bytes
	TSDB_DATA_TYPE_TINYINT    = 2  // 1 byte
	TSDB_DATA_TYPE_SMALLINT   = 3  // 2 bytes
	TSDB_DATA_TYPE_INT        = 4  // 4 bytes
	TSDB_DATA_TYPE_BIGINT     = 5  // 8 bytes
	TSDB_DATA_TYPE_FLOAT      = 6  // 4 bytes
	TSDB_DATA_TYPE_DOUBLE     = 7  // 8 bytes
	TSDB_DATA_TYPE_BINARY     = 8  // string
	TSDB_DATA_TYPE_TIMESTAMP  = 9  // 8 bytes
	TSDB_DATA_TYPE_NCHAR      = 10 // unicode string
	TSDB_DATA_TYPE_UTINYINT   = 11 // 1 byte
	TSDB_DATA_TYPE_USMALLINT  = 12 // 2 bytes
	TSDB_DATA_TYPE_UINT       = 13 // 4 bytes
	TSDB_DATA_TYPE_UBIGINT    = 14 // 8 bytes
	TSDB_DATA_TYPE_JSON       = 15
	TSDB_DATA_TYPE_VARBINARY  = 16
	TSDB_DATA_TYPE_DECIMAL    = 17
	TSDB_DATA_TYPE_BLOB       = 18
	TSDB_DATA_TYPE_MEDIUMBLOB = 19
	TSDB_DATA_TYPE_GEOMETRY   = 20
)
