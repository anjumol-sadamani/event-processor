package model

import "net/http"

type APIResponse struct {
	StatusCode int         `json:"status_code"`
	Data       interface{} `json:"data"`
}

type APIResponseEventsByDay struct {
	Day  int         `json:"day"`
	Data interface{} `json:"data"`
}

type ErrorMessage struct {
	Message string `json:"error_message"`
}

func SuccessResponse(data interface{}) *APIResponse {
	return &APIResponse{
		Data:       data,
		StatusCode: http.StatusOK,
	}
}

func FailureResponse(message string, code int) *APIResponse {
	errMsg := &ErrorMessage{
		Message: message,
	}
	return &APIResponse{
		Data:       errMsg,
		StatusCode: code,
	}
}
