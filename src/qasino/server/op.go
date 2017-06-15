package server

import (
	qasinotable "qasino/table"
)

type RequestOp struct {
	Op       string            `json:"op"`
	Sql      string            `json:"sql,omitempty"`
	Table    qasinotable.Table `json:"table,omitempty"`
	Update   interface{}       `json:"update,omitempty"`
	Static   interface{}       `json:"static,omitempty"`
	Persist  interface{}       `json:"persist,omitempty"`
	KeyCols  interface{}       `json:"keycols,omitempty"`
	Identity interface{}       `json:"identity,omitempty"`
}

type MaxWidths map[string]int

type ResponseOp struct {
	ResponseOp   string            `json:"response_op"`
	Identity     string            `json:"identity"`
	ErrorMessage string            `json:"error_message,omitempty"`
	Table        *SelectResultData `json:"table,omitempty"`
	MaxWidths    *MaxWidths        `json:"max_widths,omitempty"`
}

func ErrorResponse(msg, identity string) *ResponseOp {
	return &ResponseOp{
		ResponseOp:   "error",
		Identity:     identity,
		ErrorMessage: msg,
	}
}

func BasicSuccessResponse(identity string) *ResponseOp {
	return &ResponseOp{
		ResponseOp: "ok",
		Identity:   identity,
	}
}
func DataSuccessResponse(result *SelectResult, identity string) *ResponseOp {
	max_widths := MaxWidths(result.MaxWidths)
	if result != nil {
		return &ResponseOp{
			ResponseOp: "result_table",
			Identity:   identity,
			Table:      &result.Data,
			MaxWidths:  &max_widths,
		}
	} else {
		return &ResponseOp{
			ResponseOp: "ok",
			Identity:   identity,
		}
	}
}
