package integration_test

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"golang.org/x/net/http2"

	"github.com/ninibe/bigduration"
	"github.com/ninibe/netlog"
	"github.com/ninibe/netlog/transport"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration Suite")
}

var dataDir string

var _ = BeforeSuite(func(done Done) {

	topSettings := netlog.TopicSettings{
		SegAge:           bigduration.BigDuration{Nanos: bigduration.Month},
		SegSize:          1024 * 1024 * 1024,
		BatchNumMessages: 100,
		BatchInterval:    bigduration.BigDuration{Nanos: 200 * time.Millisecond},
		CompressionType:  netlog.CompressionType(1),
	}

	defer close(done)

	var err error

	dataDir, err = ioutil.TempDir("", "")

	nl, err := netlog.NewNetLog(dataDir, netlog.DefaultTopicSettings(topSettings), netlog.MonitorInterval(bigduration.BigDuration{Nanos: 10 * time.Second}))

	var server http.Server
	server.Addr = "localhost:12345"
	err = http2.ConfigureServer(&server, nil)
	Expect(err).ToNot(HaveOccurred())

	go func() {
		fmt.Println("started")
		http.Handle("/", transport.NewHTTPTransport(nl))
		err = server.ListenAndServe()
		Expect(err).ToNot(HaveOccurred())
	}()

	for {
		_, err := http.Get("http://localhost:12345/")
		if err == nil {
			break
		}
		time.Sleep(time.Millisecond * 10)
	}

})

var _ = AfterSuite(func(done Done) {
	defer close(done)
	err := os.RemoveAll(dataDir)
	Expect(err).ToNot(HaveOccurred())
})
