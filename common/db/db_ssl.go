// +build ssl

package db

import (
	"github.com/dezmodue/mongo-tools/common/db/openssl"
	"github.com/dezmodue/mongo-tools/common/options"
)

func init() {
	GetConnectorFuncs = append(GetConnectorFuncs, getSSLConnector)
}

// return the SSL DB connector if using SSL, otherwise, return nil.
func getSSLConnector(opts options.ToolOptions) DBConnector {
	if opts.SSL.UseSSL {
		return &openssl.SSLDBConnector{}
	}
	return nil
}
