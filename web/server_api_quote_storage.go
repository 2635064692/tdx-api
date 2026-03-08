package main

import (
	"net/http"

	webquote "web/quote"
)

var quoteStorageSystem *webquote.QuoteStorageSystem

func handleQuoteStorageStatus(w http.ResponseWriter, r *http.Request) {
	if quoteStorageSystem == nil {
		errorResponse(w, "行情存储系统未初始化")
		return
	}
	successResponse(w, quoteStorageSystem.Status())
}
