package integration_test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/comail/go-uuid/uuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type ScannerCreateResponseBody struct {
	ID         string `json:"id"`
	Next       int    `json:"next"`
	Last       int    `json:"last"`
	Persistent bool   `json:"persistent"`
}

var _ = Describe("Scanner", func() {
	Context("When Topic Exists", func() {
		var topicName string
		BeforeEach(func() {
			topicName = uuid.NewUUID().String()
			response, err := http.Post("http://localhost:12345/"+topicName, "text/plain", nil)
			Expect(err).ToNot(HaveOccurred())
			err = response.Body.Close()
			Expect(err).ToNot(HaveOccurred())
		})
		Context("When I create a scanner", func() {
			var response *http.Response
			BeforeEach(func() {
				var err error
				response, err = http.Post(fmt.Sprintf("http://localhost:12345/%s/scanner", topicName), "text/plain", nil)
				Expect(err).ToNot(HaveOccurred())
			})

			AfterEach(func() {
				err := response.Body.Close()
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should return 201 status code", func() {
				Expect(response.StatusCode).To(Equal(201))
			})

			It("Should return information", func() {

				data, err := ioutil.ReadAll(response.Body)
				Expect(err).ToNot(HaveOccurred())

				body := &ScannerCreateResponseBody{}
				err = json.Unmarshal(data, body)
				Expect(err).ToNot(HaveOccurred())

				By("returning a non-empty id")
				Expect(body.ID).ToNot(BeEmpty())

				By("returning next with value 0")
				Expect(body.Next).To(Equal(0))

				By("returning last with value 0")
				Expect(body.Last).To(Equal(0))

				By("returning persistent to be false")
				Expect(body.Persistent).To(BeFalse())

			})

		})

		Context("When Scanner exists", func() {
			var scannerID string
			BeforeEach(func() {
				response, err := http.Post(fmt.Sprintf("http://localhost:12345/%s/scanner", topicName), "text/plain", nil)
				Expect(err).ToNot(HaveOccurred())
				Expect(response.StatusCode).To(Equal(201))
				data, err := ioutil.ReadAll(response.Body)
				Expect(err).ToNot(HaveOccurred())

				body := &ScannerCreateResponseBody{}
				err = json.Unmarshal(data, body)
				Expect(err).ToNot(HaveOccurred())
				scannerID = body.ID
			})

			Context("And I post data to the topic", func() {
				BeforeEach(func() {
					response, err := http.Post(fmt.Sprintf("http://localhost:12345/%s/payload", topicName), "application/octet-stream", strings.NewReader("test"))
					Expect(err).ToNot(HaveOccurred())
					err = response.Body.Close()
					Expect(err).ToNot(HaveOccurred())
					Expect(response.StatusCode).To(Equal(201))
				})

				Context("When I read from the scanner", func() {
					var response *http.Response
					BeforeEach(func() {
						time.Sleep(200 * time.Millisecond)
						var err error
						response, err = http.Get(fmt.Sprintf("http://localhost:12345/%s/scan?id=%s", topicName, scannerID))
						Expect(err).ToNot(HaveOccurred())
					})

					AfterEach(func() {
						err := response.Body.Close()
						Expect(err).ToNot(HaveOccurred())
					})

					It("Should return 200 status code", func() {
						Expect(response.StatusCode).To(Equal(200))
					})

					It("Should return body", func() {
						data, err := ioutil.ReadAll(response.Body)
						Expect(err).ToNot(HaveOccurred())
						Expect(string(data)).To(Equal("test"))
					})

				})

			})

		})

	})
})
