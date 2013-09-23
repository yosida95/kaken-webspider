package crawler

import (
	"errors"
)

var (
	ERR_MANY_REDIRECT    = errors.New("Many Redirect Error")
	ERR_TIMEOUT          = errors.New("Request timed out")
	ERR_DATABASE         = errors.New("Database returned an error")
	ERR_DOWNLOAD         = errors.New("Failed to download a page")
	ERR_INTERNAL         = errors.New("Occur a internal error")
	ERR_INVALIDURL       = errors.New("URL is invalid")
	ERR_INVALID_ROBOTS   = errors.New("Robots.txt is invalid format")
	ERR_NOT_HTML         = errors.New("This page is not written in HTML")
	ERR_HTML_PARSE_ERROR = errors.New("Failed to parse HTML")
)
