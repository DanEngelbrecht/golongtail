package commands

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/DanEngelbrecht/golongtail/longtaillib"
	"github.com/DanEngelbrecht/golongtail/longtailutils"
	"github.com/DanEngelbrecht/golongtail/remotestore"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type httpHandler struct {
	readOnly      bool
	authorization string
	store         longtaillib.Longtail_BlockStoreAPI
}

type getBlockComplete struct {
	w http.ResponseWriter
}

func (g *getBlockComplete) OnComplete(stored_block longtaillib.Longtail_StoredBlock, err error) {
	if err != nil {
		g.w.WriteHeader(http.StatusInternalServerError)
		msg := fmt.Sprintf("block request failed. %s", err)
		fmt.Fprintln(g.w, msg)
		fmt.Fprintln(os.Stderr, msg)
	}
	defer stored_block.Dispose()
	storedBlockBytes, err := longtaillib.WriteStoredBlockToBuffer(stored_block)
	if err != nil {
		g.w.WriteHeader(http.StatusInternalServerError)
		msg := fmt.Sprintf("failed to serialize block. %s", err)
		fmt.Fprintln(g.w, msg)
		fmt.Fprintln(os.Stderr, msg)
	}
	g.w.WriteHeader(http.StatusOK)
	g.w.Write(storedBlockBytes)
}

type getExistingContentComplete struct {
	w http.ResponseWriter
}

func (g *getExistingContentComplete) OnComplete(storeIndex longtaillib.Longtail_StoreIndex, err error) {
	if err != nil {
		g.w.WriteHeader(http.StatusInternalServerError)
		msg := fmt.Sprintf("store index request failed. %s", err)
		fmt.Fprintln(g.w, msg)
		fmt.Fprintln(os.Stderr, msg)
	}
	defer storeIndex.Dispose()
	storedIndexBytes, err := longtaillib.WriteStoreIndexToBuffer(storeIndex)
	if err != nil {
		g.w.WriteHeader(http.StatusInternalServerError)
		msg := fmt.Sprintf("failed to serialize store index. %s", err)
		fmt.Fprintln(g.w, msg)
		fmt.Fprintln(os.Stderr, msg)
	}
	g.w.WriteHeader(http.StatusOK)
	g.w.Write(storedIndexBytes)
}

// Need to att http-client block store implementation

// get/index payload: chunk_hashes[]
// preflight payload: block_hashes[]
// get/block/block-hash
// get/stats
// prune
// flush
// put/block/block-hash payload: stored_block

func (h httpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fmt.Println("ServeHTTP")
	if h.authorization != "" && r.Header.Get("Authorization") != h.authorization {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	fmt.Printf("ServeHTTP %s\n", r.Method)
	switch r.Method {
	case "GET":
		h.get(r.URL.Path, w, r)
	case "PREFLIGHT":
		h.preflight(r.URL.Path, w)
	case "PUT":
		h.put(r.URL.Path, w, r)
	case "PRUNE":
		h.preflight(r.URL.Path, w)
	case "FLUSH":
		h.preflight(r.URL.Path, w)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte("only GET, PUT and HEAD are supported"))
	}
}

func (h httpHandler) get(path string, w http.ResponseWriter, r *http.Request) {
	fmt.Printf("get %s\n", path)
	splitPath := strings.Split(path, "/")
	if len(splitPath) == 1 {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "Serving %s", "a store")
		return
	}

	switch splitPath[1] {
	case "favicon.ico":
		w.WriteHeader(http.StatusNotFound)
		return
	case "stats":
		stats, err := h.store.GetStats()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			msg := fmt.Sprintf("failed to retrieve stats:%s", err)
			fmt.Fprintln(w, msg)
			fmt.Fprintln(os.Stderr, msg)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "GetStoredBlock_Count:          %s\n", longtailutils.ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Count]))
		fmt.Fprintf(w, "GetStoredBlock_RetryCount:     %s\n", longtailutils.ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_RetryCount]))
		fmt.Fprintf(w, "GetStoredBlock_FailCount:      %s\n", longtailutils.ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_FailCount]))
		fmt.Fprintf(w, "GetStoredBlock_Chunk_Count:    %s\n", longtailutils.ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Chunk_Count]))
		fmt.Fprintf(w, "GetStoredBlock_Byte_Count:     %s\n", longtailutils.ByteCountBinary(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStoredBlock_Byte_Count]))
		fmt.Fprintf(w, "PutStoredBlock_Count:          %s\n", longtailutils.ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Count]))
		fmt.Fprintf(w, "PutStoredBlock_RetryCount:     %s\n", longtailutils.ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_RetryCount]))
		fmt.Fprintf(w, "PutStoredBlock_FailCount:      %s\n", longtailutils.ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_FailCount]))
		fmt.Fprintf(w, "PutStoredBlock_Chunk_Count:    %s\n", longtailutils.ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Chunk_Count]))
		fmt.Fprintf(w, "PutStoredBlock_Byte_Count:     %s\n", longtailutils.ByteCountBinary(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PutStoredBlock_Byte_Count]))
		fmt.Fprintf(w, "GetExistingContent_Count:      %s\n", longtailutils.ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetExistingContent_Count]))
		fmt.Fprintf(w, "GetExistingContent_RetryCount: %s\n", longtailutils.ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetExistingContent_RetryCount]))
		fmt.Fprintf(w, "GetExistingContent_FailCount:  %s\n", longtailutils.ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetExistingContent_FailCount]))
		fmt.Fprintf(w, "PreflightGet_Count:            %s\n", longtailutils.ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PreflightGet_Count]))
		fmt.Fprintf(w, "PreflightGet_RetryCount:       %s\n", longtailutils.ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PreflightGet_RetryCount]))
		fmt.Fprintf(w, "PreflightGet_FailCount:        %s\n", longtailutils.ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_PreflightGet_FailCount]))
		fmt.Fprintf(w, "Flush_Count:                   %s\n", longtailutils.ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_Flush_Count]))
		fmt.Fprintf(w, "Flush_FailCount:               %s\n", longtailutils.ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_Flush_FailCount]))
		fmt.Fprintf(w, "GetStats_Count:                %s\n", longtailutils.ByteCountDecimal(stats.StatU64[longtaillib.Longtail_BlockStoreAPI_StatU64_GetStats_Count]))
		return
	case "index":
		// Get existing content index
		contentLengthHeader, hasHeader := r.Header["Content-Length"]
		contentLength := int64(0)
		var err error
		if hasHeader && len(contentLengthHeader) > 0 {
			lenStr := contentLengthHeader[0]
			contentLength, err = strconv.ParseInt(lenStr, 10, 32)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				msg := fmt.Sprintf("content length is not set for request. %s", err)
				fmt.Fprintln(w, msg)
				fmt.Fprintln(os.Stderr, msg)
				return
			}
		}
		chunksData := make([]byte, contentLength)
		if contentLength > 0 {
			_, err = r.Body.Read(chunksData)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				msg := fmt.Sprintf("content body does not match length for request. %s", err)
				fmt.Fprintln(w, msg)
				fmt.Fprintln(os.Stderr, msg)
				return
			}
		}
		chunkHashes := make([]uint64, contentLength/8)
		err = binary.Read(bytes.NewReader(chunksData), binary.LittleEndian, chunkHashes)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			msg := fmt.Sprintf("content body is malformed in request. %s", err)
			fmt.Fprintln(w, msg)
			fmt.Fprintln(os.Stderr, msg)
			return
		}
		completer := &getExistingContentComplete{w: w}
		err = h.store.GetExistingContent(chunkHashes, 0, longtaillib.CreateAsyncGetExistingContentAPI(completer))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			msg := fmt.Sprintf("can not get existing store index for request. %s", err)
			fmt.Fprintln(w, msg)
			fmt.Fprintln(os.Stderr, msg)
			return
		}
		return
	case "block":
		if len(splitPath) < 3 {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("missing block hash"))
			return
		}
		blockHashStr := splitPath[2]
		blockHash, err := strconv.ParseUint(blockHashStr, 16, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			msg := fmt.Sprintf("malformed block hash. %s", err)
			fmt.Fprintln(w, msg)
			fmt.Fprintln(os.Stderr, msg)
			return
		}
		completer := &getBlockComplete{w: w}
		completeApi := longtaillib.CreateAsyncGetStoredBlockAPI(completer)
		err = h.store.GetStoredBlock(blockHash, completeApi)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			msg := fmt.Sprintf("failed to issue request for block %s. %s", blockHashStr, err)
			fmt.Fprintln(w, msg)
			fmt.Fprintln(os.Stderr, msg)
			return
		}
		return
	}
}

func (h httpHandler) preflight(path string, w http.ResponseWriter) {
}

func (h httpHandler) put(path string, w http.ResponseWriter, r *http.Request) {
	if h.readOnly {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("store is read only"))
		return
	}
}

func (h httpHandler) prune(path string, w http.ResponseWriter) {
}

func (h httpHandler) flush(path string, w http.ResponseWriter) {
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

	jobs := longtaillib.CreateBikeshedJobAPI(uint32(numWorkerCount), 0)
	defer jobs.Dispose()
	creg := longtaillib.CreateFullCompressionRegistry()
	defer creg.Dispose()
	hashRegistry := longtaillib.CreateFullHashRegistry()
	defer hashRegistry.Dispose()

	mode := remotestore.ReadWrite
	if readOnly {
		mode = remotestore.ReadOnly
	}
	remoteIndexStore, err := remotestore.CreateBlockStoreForURI(blobStoreURI, "", jobs, numWorkerCount, mode)
	if err != nil {
		return storeStats, timeStats, errors.Wrap(err, fname)
	}
	defer remoteIndexStore.Dispose()

	auth := os.Getenv("LONGTAIL_SERVER_HTTP_AUTH")
	addresses := []string{":http"}

	handler := httpHandler{readOnly: readOnly, authorization: auth, store: remoteIndexStore}
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
