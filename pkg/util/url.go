package util

import (
	"fmt"
	"regexp"
)

type UrlParams struct {
	Protocol, User, Pass, Host, Port string
	Extra                            *string
}

func CreateConnectionUrl(params UrlParams) (url, obscured string) {
	extra := ""

	if params.Extra != nil {
		extra += *params.Extra
	}

	url = fmt.Sprintf(
		"%s://%s:%s@%s:%s/%s",
		params.Protocol,
		params.User,
		params.Pass,
		params.Host,
		params.Port,
		extra,
	)

	re := regexp.MustCompile(".")

	obscured = fmt.Sprintf(
		"%s://%s:%s@%s:%s/%s",
		params.Protocol,
		params.User,
		re.ReplaceAllString(params.Pass, "x"),
		params.Host,
		params.Port,
		extra,
	)

	return
}
