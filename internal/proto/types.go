// Package proto defines the RESP (REdis Serialization Protocol) type system.
//
// RESP3 type prefixes:
//
//	'+' – Simple String
//	'-' – Simple Error
//	':' – Integer
//	'$' – Bulk String  (length-prefixed, may be null: $-1\r\n)
//	'*' – Array        (may be null: *-1\r\n)
//	'_' – Null         (RESP3)
//	'#' – Boolean      (RESP3)
//	',' – Double       (RESP3)
//	'(' – Big Number   (RESP3)
//	'!' – Blob Error   (RESP3)
//	'=' – Verbatim String (RESP3)
//	'%' – Map          (RESP3)
//	'~' – Set          (RESP3)
//	'|' – Attribute    (RESP3)
//	'>' – Push         (RESP3)
package proto

// RESPType identifies which RESP value variant is held by a Value.
type RESPType byte

const (
	TypeSimpleString  RESPType = '+'
	TypeSimpleError   RESPType = '-'
	TypeInteger       RESPType = ':'
	TypeBulkString    RESPType = '$'
	TypeArray         RESPType = '*'
	TypeNull          RESPType = '_' // RESP3
	TypeBoolean       RESPType = '#' // RESP3
	TypeDouble        RESPType = ',' // RESP3
	TypeBigNumber     RESPType = '(' // RESP3
	TypeBlobError     RESPType = '!' // RESP3
	TypeVerbatimStr   RESPType = '=' // RESP3
	TypeMap           RESPType = '%' // RESP3
	TypeSet           RESPType = '~' // RESP3
	TypeAttribute     RESPType = '|' // RESP3
	TypePush          RESPType = '>' // RESP3
)

// String returns the human-readable name of the type.
func (t RESPType) String() string {
	switch t {
	case TypeSimpleString:
		return "SimpleString"
	case TypeSimpleError:
		return "SimpleError"
	case TypeInteger:
		return "Integer"
	case TypeBulkString:
		return "BulkString"
	case TypeArray:
		return "Array"
	case TypeNull:
		return "Null"
	case TypeBoolean:
		return "Boolean"
	case TypeDouble:
		return "Double"
	case TypeBigNumber:
		return "BigNumber"
	case TypeBlobError:
		return "BlobError"
	case TypeVerbatimStr:
		return "VerbatimString"
	case TypeMap:
		return "Map"
	case TypeSet:
		return "Set"
	case TypeAttribute:
		return "Attribute"
	case TypePush:
		return "Push"
	default:
		return "Unknown"
	}
}
