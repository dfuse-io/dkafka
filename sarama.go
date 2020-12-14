package dkafka

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
)

func tlsConfig(caCertFile string) (*tls.Config, error) {
	tlsConfig := &tls.Config{}
	caCertPool := x509.NewCertPool()
	tlsConfig.RootCAs = caCertPool
	if caCertFile == "" {
		return tlsConfig, nil
	}

	caCert, err := ioutil.ReadFile(caCertFile)
	if err != nil {
		return nil, err
	}
	caCertPool.AppendCertsFromPEM(caCert)
	return tlsConfig, nil
}

func addClientCert(clientCertFile, clientKeyFile string, tlsConfig *tls.Config) error {
	// Load client cert
	cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
	if err != nil {
		return err
	}
	tlsConfig.Certificates = []tls.Certificate{cert}
	return nil
}
