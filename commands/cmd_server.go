package commands

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/sirupsen/logrus"
)

type httpHandler struct {
	readOnly      bool
	authorization string
}

func (h httpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fmt.Println("ServeHTTP")
	if h.authorization != "" && r.Header.Get("Authorization") != h.authorization {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	switch r.Method {
	case "GET":
		h.get(r.URL.Path, w)
	case "HEAD":
		h.head(r.URL.Path, w)
	case "PUT":
		h.put(r.URL.Path, w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte("only GET, PUT and HEAD are supported"))
	}
}

func (h httpHandler) get(path string, w http.ResponseWriter) {
}

func (h httpHandler) head(path string, w http.ResponseWriter) {
}

func (h httpHandler) put(path string, w http.ResponseWriter, r *http.Request) {
	if h.readOnly {
		w.WriteHeader(http.StatusBadRequest)
		msg := fmt.Sprintf("store is read only")
		fmt.Fprintln(w, msg)
		return
	}
	return
}

func serve(
	numWorkerCount int,
	blobStoreURI string,
	readOnly bool,
	mutualTLS bool,
	clientCA string,
	cert string,
	key string) ([]longtailutils.StoreStat, []longtailutils.TimeStat, error) {
	const fname = "serve"
	log := logrus.WithContext(context.Background()).WithFields(logrus.Fields{
		"fname":          fname,
		"numWorkerCount": numWorkerCount,
		"blobStoreURI":   blobStoreURI,
	})
	log.Debug(fname)

	storeStats := []longtailutils.StoreStat{}
	timeStats := []longtailutils.TimeStat{}

	auth := os.Getenv("LONGTAIL_SERVER_HTTP_AUTH")
	addresses := []string{":http"}

	handler := httpHandler{readOnly: readOnly, authorization: auth}
	http.Handle("/", handler)

	tlsConfig := &tls.Config{}
	if mutualTLS {
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}

	if clientCA != "" {
		certPool := x509.NewCertPool()
		b, err := ioutil.ReadFile(clientCA)
		if err != nil {
			return storeStats, timeStats, err
		}
		if ok := certPool.AppendCertsFromPEM(b); !ok {
			return storeStats, timeStats, errors.New("no client CA certficates found in client-ca file")
		}
		tlsConfig.ClientCAs = certPool
	}

	ctx := context.Background()

	// Run the server(s) in a goroutine, and use the main goroutine to wait for
	// a signal or a failing server (ctx gets cancelled in that case)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for _, addr := range addresses {
		go func(a string) {
			server := &http.Server{
				Addr:      a,
				TLSConfig: tlsConfig,
				//ErrorLog:  log.New(stderr, "", log.LstdFlags),
			}
			var err error
			if key == "" {
				err = server.ListenAndServe()
			} else {
				err = server.ListenAndServeTLS(cert, key)
			}
			if err != nil {
				log.WithError(err).Error("serve failed")
			}
			cancel()
		}(addr)
	}
	// wait for either INT/TERM or an issue with the server
	<-ctx.Done()

	return storeStats, timeStats, nil
}

type ServeCmd struct {
	StorageURIOption
	ReadOnly  bool   `name:"read-only" help:""`
	MutualTLS bool   `name:"mutual-tls" help:""`
	ClientCA  string `name:"client-ca" help:""`
	Cert      string `name:"cert" help:""`
	Key       string `name:"key" help:""`
}

func (r *ServeCmd) Run(ctx *Context) error {
	storeStats, timeStats, err := serve(
		ctx.NumWorkerCount,
		r.StorageURI,
		r.ReadOnly,
		r.MutualTLS,
		r.ClientCA,
		r.Cert,
		r.Key)
	ctx.StoreStats = append(ctx.StoreStats, storeStats...)
	ctx.TimeStats = append(ctx.TimeStats, timeStats...)
	return err
}
