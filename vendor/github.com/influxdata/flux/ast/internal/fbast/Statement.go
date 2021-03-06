// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package fbast

type Statement = byte

const (
	StatementNONE                Statement = 0
	StatementBadStatement        Statement = 1
	StatementVariableAssignment  Statement = 2
	StatementMemberAssignment    Statement = 3
	StatementExpressionStatement Statement = 4
	StatementReturnStatement     Statement = 5
	StatementOptionStatement     Statement = 6
	StatementBuiltinStatement    Statement = 7
	StatementTestStatement       Statement = 8
)

var EnumNamesStatement = map[Statement]string{
	StatementNONE:                "NONE",
	StatementBadStatement:        "BadStatement",
	StatementVariableAssignment:  "VariableAssignment",
	StatementMemberAssignment:    "MemberAssignment",
	StatementExpressionStatement: "ExpressionStatement",
	StatementReturnStatement:     "ReturnStatement",
	StatementOptionStatement:     "OptionStatement",
	StatementBuiltinStatement:    "BuiltinStatement",
	StatementTestStatement:       "TestStatement",
}
