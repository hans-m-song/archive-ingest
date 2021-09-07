package util

import (
	"fmt"
	"regexp"
)

type UrlParams struct {
	Protocol, User, Pass, Host, Port, Path string
}

func CreateConnectionUrl(params UrlParams) (string, string) {

	url := fmt.Sprintf(
		"%s://%s:%s@%s:%s/%s",
		params.Protocol,
		params.User,
		params.Pass,
		params.Host,
		params.Port,
		params.Path,
	)

	re := regexp.MustCompile(".")

	obscured := fmt.Sprintf(
		"%s://%s:%s@%s:%s/%s",
		params.Protocol,
		params.User,
		re.ReplaceAllString(params.Pass, "x"),
		params.Host,
		params.Port,
		params.Path,
	)

	return url, obscured
}
