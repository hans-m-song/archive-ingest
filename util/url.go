package util

import (
	"fmt"
	"regexp"
)

type UrlParams struct {
	Protocol, User, Pass, Host, Port string
}

func CreateConnectionUrl(params UrlParams) (url, obscured string) {
	url = fmt.Sprintf(
		"%s://%s:%s@%s:%s",
		params.Protocol,
		params.User,
		params.Pass,
		params.Host,
		params.Port,
	)

	re := regexp.MustCompile(".")

	obscured = fmt.Sprintf(
		"%s://%s:%s@%s:%s",
		params.Protocol,
		params.User,
		re.ReplaceAllString(params.Pass, "x"),
		params.Host,
		params.Port,
	)

	return
}
